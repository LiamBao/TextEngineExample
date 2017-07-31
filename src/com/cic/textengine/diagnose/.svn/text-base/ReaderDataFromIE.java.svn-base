package com.cic.textengine.diagnose;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;

import com.cic.lucene.SearchResult;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;
import com.cic.tmsearch.ILeoReportSearcher;
import com.cic.tmsearch.LeoReportSearcherFactory;

public class ReaderDataFromIE {

	public static void search(String ip, String dp, String key, String path, int beginPage) throws ParseException, DecoderException {

		PartitionKey parKey = PartitionKey.decodeStringKey(key);
		
		LeoReportSearcherFactory.initLocalSearcher("192.168.2.210");
		ILeoReportSearcher searcher = LeoReportSearcherFactory.getSearcher();
		String sql = "";

		int first = 0;
		int size = 100;
		String[] ips = {ip};
		String[] dps = {dp};
		String[] resultFields = new String[] {"Subject","Content","KeyTerm","KeyWord","DateOfPost","ItemUrl","Poster","IsTopicPost","ForumName","ForumUrl","Source","ItemType","ITEM_ID","ITEM_KEY","SimpleDateOfPost","DateOfPostSort","IsQuotation","SeedingInfoId","TopicItemID","NumOfReply","NumOfPoster","ThreadID","PosterID","SysSiteID","SiteID","ForumID"};
		
		SearchResult searchResult = searcher.search(ips, dps, sql, first, size, resultFields, null, null, null);
		int total = searchResult.getTotalHits();
		int page = total/size + 1;
		System.out.println(String.format("Totally %s items and %s pages to go.",total, page));
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.0");
		
		ArrayList<TEItem> teItemList = new ArrayList<TEItem>();
		for(int i=beginPage; i<page; i++){
			searchResult = searcher.search(ips, dps, sql, i*size, size, resultFields, null, null, null);
			for (HashMap<String, String> item : searchResult.getItems()) {
				// test if the item belongs to specific forum
				ItemKey itemKey = ItemKey.decodeKey(item.get("ITEM_KEY"));
				if(itemKey.getPartitionKey().equals(key)){
					TEItem teItem = new TEItem();
					teItem.setSubject(item.get("Subject"));
					teItem.setContent(item.get("Content"));
					TEItemMeta meta = new TEItemMeta();
					meta.setDateOfPost(sdf.parse(item.get("DateOfPost")).getTime());
					meta.setFirstExtractionDate(System.currentTimeMillis());
					meta.setForumID(itemKey.getForumID());
					meta.setForumName(item.get("ForumName"));
					meta.setForumUrl(item.get("ForumUrl"));
					meta.setItem(teItem);
					meta.setItemID(itemKey.getItemID());
					meta.setItemType(item.get("ItemType"));
					meta.setItemUrl(item.get("ItemUrl"));
					meta.setKeyword(item.get("KeyWord"));
					meta.setKeywordGroup(item.get("KeyTerm"));
					meta.setLatestExtractionDate(sdf.parse(item.get("DateOfPost")).getTime());
					meta.setPoster(item.get("Poster"));
					meta.setPosterID(item.get("PosterID"));
					meta.setSimpleDateOfPost(item.get("SimpleDateOfPost"));
					meta.setSiteID(Long.parseLong(itemKey.getSiteID()));
					meta.setSiteName(item.get("SiteName"));
					meta.setSource(item.get("Source"));
					meta.setSubject(item.get("Subject"));
					try{
						meta.setThreadID(Long.parseLong(item.get("ThreadID")));
					} catch (Exception e){
						meta.setThreadID(0);
					}
					meta.setTopicPost(item.get("IsTopicPost").equals("0") ? false: true);
					
					teItem.setMeta(meta);
					teItemList.add(teItem);
				}
			}
			if(i % 100 == 0){
				System.out.println(String.format("%s queries finished, %s items found.", i, teItemList.size()));
			}
		}
		
		
		// sort the list according to the item id
		Collections.sort(teItemList, new Comparator<TEItem>(){
			public int compare(TEItem t1, TEItem t2) {
			     if (t1.getMeta().getItemID()<t2.getMeta().getItemID()){
			    	 return -1;
			     } else if (t1.getMeta().getItemID()>t2.getMeta().getItemID()){
			    	 return 1;
			     } else 
			    	 return 0;
			  };
		});

		for(int i=0; i<teItemList.size(); i++){
			System.out.println(teItemList.get(i).getMeta().getItemID());
		}
		
		// write to local IDFs
		RepositoryEngine engine;
		try {
			engine = RepositoryFactory.getNewRepositoryEngineInstance(path);
			PartitionWriter pw = null;
			long startItemID = 1;
			long lastItemId = 0;
			pw = engine.getPartitionWriter(parKey.getYear(), parKey.getMonth(), parKey.getSiteID(), parKey.getForumID(), startItemID);
			
			for (int idx = 0;idx < teItemList.size();idx++){
				TEItem item = teItemList.get(idx);
				long itemId = item.getMeta().getItemID();
				while(lastItemId + 1 != itemId){
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

	public static void main(String[] args) throws IOException, ParseException, DecoderException {

		/*使用说明
		 * args[0]指定该项目所在的机器, 例如: 192.168.2.201:7163
		 * args[1]指定要搜索的项目某个年月的索引文件名，例如: 200907Prj105
		 * args[2]指定TE partition Key，例如: 4646343838_2009_46494432464944_7
		 * args[3]指定IDF文件写入地址
		 */

		if(args.length<5){
			System.out.println("5 parameters needed: IPs, DPs, PartitionKey, Repository, BeginPage");
			return;
		}
		search(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]));
	}
}