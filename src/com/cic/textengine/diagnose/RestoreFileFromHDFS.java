package com.cic.textengine.diagnose;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cic.store.metaheader.MetaHeader;
import cic.store.reader.RecordReader;

import com.cic.DFSUtil.FileHandler;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class RestoreFileFromHDFS {

	
	public static void getDataFromHDFS(int monthid, int prjid, String key, String teRepoPath) throws IOException, DecoderException{
		Configuration conf=new Configuration();
		conf.addResource(new Path("properties/hadoop-site.xml"));
		FileSystem fs=FileSystem.get(conf);
		
		String localpath = String.format("/user/newstore/%d/%d_%d/", monthid, prjid, monthid);		
		
		String path = FileHandler.onlyRead(fs,localpath);
		
		FSDataInputStream meta=fs.open(new Path(path+"/meta.dat"));
		FSDataInputStream data=fs.open(new Path(path+"/data.dat"));
		MetaHeader header = MetaHeader.getMetaHeader(meta);
		RecordReader reader = new RecordReader(header,meta,data);
		
		ArrayList<TEItem> teItemList = new ArrayList<TEItem>();
		int count = 0;
		for (cic.store.reader.ResultSet ret : reader){
			ItemKey itemKey = ItemKey.decodeKey(ret.getString("TEKey"));
			if(itemKey.getPartitionKey().equals(key)){
				TEItem teItem = new TEItem();
				teItem.setSubject(ret.getString("Subject"));
				teItem.setContent(ret.getString("Content"));
				TEItemMeta temeta = new TEItemMeta();
				temeta.setDateOfPost(ret.getLong("DateOfPost"));
				temeta.setFirstExtractionDate(System.currentTimeMillis());
				temeta.setForumID(itemKey.getForumID());
				temeta.setForumName(ret.getString("ForumName"));
				temeta.setForumUrl(ret.getString("ForumUrl"));
				temeta.setItem(teItem);
				temeta.setItemID(itemKey.getItemID());
				temeta.setItemType(ret.getString("ItemType"));
				temeta.setItemUrl(ret.getString("ItemUrl"));
				temeta.setKeyword(ret.getString("KeyWord"));
				temeta.setKeywordGroup(ret.getString("KeywordGroup"));
				temeta.setLatestExtractionDate(ret.getLong("DateOfPost"));
				temeta.setPoster(ret.getString("Poster"));
				temeta.setPosterID(ret.getString("PosterID"));
				temeta.setSimpleDateOfPost(getSimpleDateofPost(ret.getLong("DateOfPost")));
				temeta.setSiteID(Long.parseLong(itemKey.getSiteID()));
				temeta.setSiteName(ret.getString("SiteName"));
				temeta.setSource(ret.getString("Source"));
				temeta.setSubject(ret.getString("Subject"));
				temeta.setThreadID(ret.getLong("ThreadID"));
				temeta.setTopicPost(ret.getBool("IsTopicPost"));
				
				teItem.setMeta(temeta);
				teItemList.add(teItem);
			}
			count ++;
			if (count % 500 == 0)
				System.out.println(String.format("%s items read, %s items found.", count, teItemList.size()));
		}
		meta.close();
		data.close();
		fs.close();
		
		// sort the list according to the item id
		Collections.sort(teItemList, new Comparator<TEItem>() {
			public int compare(TEItem t1, TEItem t2) {
				if (t1.getMeta().getItemID() < t2.getMeta().getItemID()) {
					return -1;
				} else if (t1.getMeta().getItemID() > t2.getMeta().getItemID()) {
					return 1;
				} else
					return 0;
			};
		});

		for (int i = 0; i < teItemList.size(); i++) {
			System.out.println(teItemList.get(i).getMeta().getItemID());
		}
		
		System.out.println(String.format("Totally %s items are retrived from HDFS.", teItemList.size()));

		// write to local IDFs
		PartitionKey parKey = PartitionKey.decodeStringKey(key);
		RepositoryEngine engine;
		try {
			engine = RepositoryFactory.getNewRepositoryEngineInstance(teRepoPath);
			PartitionWriter pw = null;
			long startItemID = 1;
			long lastItemId = 0;
			pw = engine.getPartitionWriter(parKey.getYear(), parKey.getMonth(),
					parKey.getSiteID(), parKey.getForumID(), startItemID);

			for (int idx = 0; idx < teItemList.size(); idx++) {
				TEItem item = teItemList.get(idx);
				long itemId = item.getMeta().getItemID();
				while (lastItemId + 1 != itemId) {
					pw.writeItem(item);
					lastItemId++;
				}
				pw.writeItem(item);
				lastItemId = item.getMeta().getItemID();
			}
			pw.flush();
			pw.close();
		} catch (RepositoryEngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String getSimpleDateofPost(long dateOfPost){
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(dateOfPost);
		int yearOfPost = calendar.get(Calendar.YEAR);
		int monthOfPost = calendar.get(Calendar.MONTH) + 1;
		int dayOfPost = calendar.get(Calendar.DAY_OF_MONTH);		
		
		/* To fix the bug occuring when importing data of 2006-12 */
		if (yearOfPost == 1970){
			yearOfPost = 2006;
			monthOfPost = 12;
		}
		
		String simpleDateOfPost = String.format("%d%02d%02d", yearOfPost, monthOfPost, dayOfPost);
		return simpleDateOfPost;
	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws DecoderException 
	 */
	public static void main(String[] args) throws IOException, DecoderException {
//		int monthid = 112;
//		int prjid = 610;
//		String parkey = "534552413133_2013_363130_4";
//		String teRepoPath = "/users/sunyuz/Desktop";
		if(args.length < 4){
			System.out.println(String.format("Usage, 4 parameters needed: monthid, projectid, partiition key, te repo path"));
			return;
		}
		int monthid = Integer.parseInt(args[0].trim());
		int prjid = Integer.parseInt(args[1].trim());
		String parkey = args[2].trim();
		String teRepoPath = args[3].trim();
		getDataFromHDFS(monthid, prjid, parkey, teRepoPath);

	}

}
