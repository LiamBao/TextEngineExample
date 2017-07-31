package com.cic.textengine.diagnose;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

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
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionWrite;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class DNDataLostCheck {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * @throws DecoderException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, DecoderException {
		
		String url = "jdbc:mysql://192.168.2.2:3306/TENameNodeRepository";
		String user = "TENN002";
		String pwd = "Vj3tRws2";
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection(url, user, pwd);
		Statement st = conn.createStatement();
		
		url = "jdbc:mysql://192.168.2.2:3306/TEPostTrend";
		Connection connPt = DriverManager.getConnection(url, user, pwd);
		Statement stPt = connPt.createStatement();
		
		ResultSet rs = null;
		
		// 读取partition id 列表
		ArrayList<Long> idlist = getIDList();
		
		ArrayList<Long> restoreList = new ArrayList<Long>();
		ArrayList<Long> lowerversionList = new ArrayList<Long>();
		HashMap<Long, PartitionVersionCount> partitionInfoList = new HashMap<Long, PartitionVersionCount>();
		
		String dnkey = "d7a1c9f4-31f2-449b-9d14-74ceea86194d";
		// 检查partition 在其他DN上的数据是否大于交付的数据
		for(long id : idlist){
			String sql = String.format("SELECT * FROM T_DN_PARTITION WHERE PARTITION_ID = %s AND DN_KEY != '%s' ORDER BY VERSION DESC LIMIT 1", id, dnkey);
			st.execute(sql);
			rs = st.getResultSet();
			int dnCount = 0;
			int dnVersion = 0;
			int ptCount = 0;
			if(rs.next()){
				dnCount = rs.getInt("PARTITION_ITEM_COUNT");
				dnVersion = rs.getInt("VERSION");
			}
			if(dnCount == 0){
				restoreList.add(id);
				continue;
			}
			
			sql = String.format("SELECT * FROM T_PARTITION WHERE PARTITION_ID = %s", id);
			st.execute(sql);
			rs = st.getResultSet();
			int year = 0;
			int month = 0;
			String siteid = "";
			String forumid = "";
			int parVersion = 0;
			if(rs.next()){
				year = rs.getInt("THE_YEAR");
				month = rs.getInt("THE_MONTH");
				siteid = rs.getString("SITE_ID");
				forumid = rs.getString("FORUM_ID");
				parVersion = rs.getInt("VERSION");
			}
			if(parVersion == dnVersion){
				System.out.println("Normal partition, continue... ID : "+id);
				continue;
			}
			
			
			sql = String.format("SELECT * FROM T_POSTTREND WHERE YEAR = %s AND MONTH = %s AND SITE_ID = '%s' AND FORUM_ID = '%s' ", year, month, siteid, forumid);
			stPt.execute(sql);
			rs = stPt.getResultSet();
			if(rs.next()){
				ptCount = rs.getInt("CHECKPOINT_ITEMID");
			}
			if (ptCount > dnCount){
				restoreList.add(id);
			} else {
				lowerversionList.add(id);
				PartitionVersionCount pvc = new PartitionVersionCount();
				pvc.partitionID = id;
				pvc.count = dnCount;
				pvc.version = dnVersion;
				partitionInfoList.put(id, pvc);
			}
		}
		System.out.println("Restore ID List:");
		for(long id : restoreList){
			System.out.println(id);
		}
		System.out.println();
		
		System.out.println("Lower Version ID List:");
		for(long id : lowerversionList){
			System.out.println(id);
		}
		
		System.out.println();
		
		// 对于需要降低版本的partition，1:partition版本，2:修改114上的版本
		for(long id : lowerversionList){
			PartitionVersionCount pvc = partitionInfoList.get(id);
			int version = pvc.version;
			int count = pvc.count;
			String sql = String.format("UPDATE T_PARTITION SET VERSION = %s, ITEM_COUNT = %s WHERE PARTITION_ID = %s",version, count, id);
			System.out.println(sql);
			st.execute(sql);
			sql = String.format("UPDATE T_DN_PARTITION SET VERSION = %s, PARTITION_ITEM_COUNT = %s WHERE PARTITION_ID = %s AND DN_KEY = '%s'", version, count, id, dnkey);
			System.out.println(sql);
			st.execute(sql);
		}
		
		restorePartitionFromHDFS(conn, restoreList);
		
		conn.close();
		connPt.close();
		

	}
	
	public static void restorePartitionFromHDFS(Connection conn, ArrayList<Long> idlist) throws SQLException, ClassNotFoundException, IOException, DecoderException{
		// 读取DCMIS交付记录看这个forum最后一次交付的项目，如果有则可以从HDFS中读取数据
		
		String url = "jdbc:mysql://192.168.1.32:3306/DCMIS";
		String user = "dcmis_opr";
		String pwd = "knr854";
		Class.forName("com.mysql.jdbc.Driver");
		Connection connDC = DriverManager.getConnection(url, user, pwd);
		Statement stDC = connDC.createStatement();
		
		url = "jdbc:mysql://192.168.2.2:3306/TEPostTrend";
		user = "TENN002";
		pwd = "Vj3tRws2";
		Connection connPt = DriverManager.getConnection(url, user, pwd);
		Statement stPt = connPt.createStatement();
		
		Statement st = conn.createStatement();
		ResultSet rs = null;
		String dnkey = "4263e641-3c09-49a1-b001-565a383a199d";
		String errordnkey = "d7a1c9f4-31f2-449b-9d14-74ceea86194d";
		
		PrintWriter pw = new PrintWriter(new FileWriter("/home/te_opr/sqlfile.txt"));
		
		PrintWriter relogpw = new PrintWriter(new FileWriter("/home/te_opr/relogPartition.log"));
		
		for(long id: idlist){
			String sql = String.format("SELECT * FROM T_PARTITION WHERE PARTITION_ID = %s", id);
			int year = 0;
			int month = 0;
			String siteid = "";
			String forumid = "";
			int siteidInt = 0;
			String source = "";
			PartitionKey parkey = null;
			
			st.execute(sql);
			rs = st.getResultSet();
			if(rs.next()){
				year = rs.getInt("THE_YEAR");
				month = rs.getInt("THE_MONTH");
				siteid = rs.getString("SITE_ID");
				forumid = rs.getString("FORUM_ID");
				parkey = new PartitionKey(year, month, siteid, forumid);
			}
			if(siteid.startsWith("FF")){
				source = "FF";
				siteidInt = Integer.parseInt(siteid.substring(2));
			} else {
				source = "SERA";
				siteidInt = Integer.parseInt(siteid.substring(4));
			}
			
			sql = String
					.format("SELECT OPERATION_ID FROM T_CHECKPOINT WHERE YEAR = %s AND MONTH = %s AND SOURCE = '%s' AND SITE_ID = %s AND FORUM_ID = '%s' ORDER BY SUBMIT_TIME LIMIT 1",
							year, month, source, siteidInt, forumid);
			stDC.execute(sql);
			rs = stDC.getResultSet();
			if(rs.next()){
				int monthid = getMonthID(year, month);
				int projectid = 0;
				sql = String.format("SELECT PROJECT_ID FROM T_OPERATION_LIST WHERE OPERATION_ID = %s", rs.getInt("OPERATION_ID"));
				stDC.execute(sql);
				rs = stDC.getResultSet();
				if (rs.next()){
					projectid = rs.getInt("PROJECT_ID");
					projectid = 871;
					long maxItemId = restorePartition(monthid, projectid, parkey.generateStringKey());
//					long maxItemId = restorePartition2(stPt, parkey);
					if(maxItemId == 0){
						System.out.println("Fail to restore data for "+id);
						continue;
					}
					int maxVersion = 0;
					sql = String.format("SELECT * FROM `T_PARTITION_OPERATION_LOG` WHERE PARTITION_ID = %s", id);
					st.execute(sql);
					rs = st.getResultSet();
					while(rs.next()){
						int type = rs.getInt("OPERATION_TYPE");
						int version = rs.getInt("VERSION");
						Timestamp ts = rs.getTimestamp("TS");
						if(type == 1){
							OLogPartitionWrite olog_write = new OLogPartitionWrite();
							olog_write.setVersion(version);
							olog_write.readFields(rs.getBinaryStream("OPERATION_DATA"));
							System.out.println(String.format("Type: Write, Version: %s, Start Item ID: %s, Item Count: %s, Timestamp: %s)",
									version, olog_write.getStartItemID(), olog_write.getItemCount(), ts));
							if(olog_write.getStartItemID() + olog_write.getItemCount() - 1 == maxItemId){
								maxVersion = version;
							}
						} else if (type == 3){
							
						}
					}
					
					if(maxVersion == 0){
						maxVersion = 1;
						sql = String
								.format("UPDATE T_PARTITION SET VERSION = %s, ITEM_COUNT = %s WHERE PARTITION_ID = %s;",
										maxVersion, maxItemId, id);
						System.out.println(sql);
						pw.println(sql);
						
						sql = String
								.format("DELETE FROM T_DN_PARTITION WHERE PARTITION_ID = %s;", id);
						System.out.println(sql);
						pw.println(sql);
						
						sql = String
								.format("INSERT INTO T_DN_PARTITION (VERSION, PARTITION_ITEM_COUNT, PARTITION_ID, DN_KEY, TS_LAST_MODIFY) VALUES ('%s', '%s', '%s', '%s', '2013-06-01 14:00:00');",
										maxVersion, maxItemId, id, dnkey);
						System.out.println(sql);
						pw.println(sql);
						
						sql = String.format("%s, %s", id, maxItemId);
						relogpw.println(sql);
					} else {
						sql = String
								.format("UPDATE T_PARTITION SET VERSION = %s, ITEM_COUNT = %s WHERE PARTITION_ID = %s;",
										maxVersion, maxItemId, id);
						System.out.println(sql);
						pw.println(sql);
//						sql = String
//								.format("INSERT INTO T_DN_PARTITION (VERSION, PARTITION_ITEM_COUNT, PARTITION_ID, DN_KEY, TS_LAST_MODIFY) VALUES ('%s', '%s', '%s', '%s', '2013-06-01 14:00:00');",
//										maxVersion, maxItemId, id, dnkey);
//						System.out.println(sql);
//						pw.println(sql);
						sql = String
								.format("UPDATE T_DN_PARTITION SET VERSION = %s, PARTITION_ITEM_COUNT = %s WHERE PARTITION_ID = %s AND DN_KEY = '%s';",
										maxVersion, maxItemId, id, errordnkey);
						System.out.println(sql);
						pw.println(sql);
					}
					
					pw.flush();
					relogpw.flush();
					
				}
			} else {
				System.out.println("Fail to restore data for "+id);
			}
		}
		pw.close();
		relogpw.close();
		connDC.close();
	}
	
	public static int getMonthID(int year, int month){
		return (year - 2004)*12 + month;
	}
	public static long restorePartition2(Statement st, PartitionKey parkey) throws SQLException{
		String sql = String.format("SELECT * FROM T_POSTTREND WHERE YEAR = %s AND MONTH = %s AND SITE_ID = '%s' AND FORUM_ID = '%s'", parkey.getYear(), parkey.getMonth(), parkey.getSiteID(), parkey.getForumID());
		st.execute(sql);
		ResultSet rs = st.getResultSet();
		if(rs.next()){
			return rs.getLong("CHECKPOINT_ITEMID");
		}
		return 0;
	}
	public static long restorePartition(int monthid, int prjid, String key) throws IOException, DecoderException{
		System.out.println(String.format("Restore month %s, project %s, partitoin %s", monthid, prjid, key));
		String teRepoPath = "/home/te_opr/";
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

		long maxItemId = 0;
		for (int i = 0; i < teItemList.size(); i++) {
//			System.out.println(teItemList.get(i).getMeta().getItemID());
			maxItemId = teItemList.get(i).getMeta().getItemID();
		}
		
		System.out.println(String.format("Totally %s items are retrived from HDFS.", teItemList.size()));
		if (maxItemId == 0)
			return maxItemId;

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
		return maxItemId;
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
	
	public static ArrayList<Long> getIDList(){
		ArrayList<Long> idlist = new ArrayList<Long>();
		idlist.add(3467122L);
		return idlist;
	}

}
class PartitionVersionCount{
	long partitionID;
	int version;
	int count;
}
