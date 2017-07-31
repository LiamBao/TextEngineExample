package com.cic.textengine.diagnose;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import com.cic.common.service.database.DB;
import com.cic.textengine.datadelivery.DcmisDB;
import com.cic.textengine.repository.type.PartitionKey;

public class QueryPostTrend {
	
	private Connection posttrendConn = null;
	private Connection dcmisConn = null;
	
	public QueryPostTrend()
	{
		try {
			DB.createConnection();
			posttrendConn = DB.getConnection();
			
			DcmisDB.createConnection();
			dcmisConn = DcmisDB.getConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public long queryPartitionPosttrend(int year, int month, String siteid, String forumid) {
		
		long partitionPost = 0;
		try {
			Statement stat = posttrendConn.createStatement();
			String sql = String.format("select * from T_POSTTREND where SITE_ID = '%s' and FORUM_ID = '%s' and YEAR = %s and MONTH = %s", siteid, forumid, year, month);
			stat.execute(sql);
			ResultSet result = stat.getResultSet();
			if(result.next())
			{
				long post = result.getLong("POST_COUNT");
				long checkpoint = result.getLong("CHECKPOINT_POST_COUNT");
				if(checkpoint != 0)
					partitionPost = checkpoint;
				else
					partitionPost = post;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return partitionPost;
	}

	public long queryProjectPosttrend(int prjID, int year, int month) {
		long prj_post = 0;
		//query all the partitions of the project
		try {
			String sql = String.format("select * from T_PROJECT_FORUM where PROJECT_ID = %s", prjID);
			Statement stat = dcmisConn.createStatement();
			ArrayList<PartitionKey> parkeyList = new ArrayList<PartitionKey>();
			stat.execute(sql);
			ResultSet result = stat.getResultSet();
			while(result.next())
			{
				String siteid = result.getString("SITE_ID");
				String forumid = result.getString("FORUM_ID");
				PartitionKey key = new PartitionKey(year, month, siteid, forumid);
				parkeyList.add(key);
			}
			
			for (PartitionKey key : parkeyList) {
				prj_post += queryPartitionPosttrend(key.getYear(), key
						.getMonth(), key.getSiteID(), key.getForumID());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return prj_post;
	}
	
	public void close()
	{
		DcmisDB.close();
		DB.close();
	}
	
	public static void main(String[] args)
	{
		try {
			int year = 2008;
			int month = 8;
			ArrayList<PartitionKey> parkeyList = new ArrayList<PartitionKey>();
			FileReader fr = new FileReader("/home/joe.sun/Desktop/35import");
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while((line=br.readLine())!=null)
			{
				String[] strArray = line.split("\t");
				String siteid = "FF"+strArray[1].trim();
				String forumid = strArray[2].trim();
				PartitionKey key = new PartitionKey(year, month, siteid, forumid);
				parkeyList.add(key);
			}
			br.close();
			fr.close();
			
			QueryPostTrend query = new QueryPostTrend();
			long totalCount = 0;
			for(PartitionKey key : parkeyList)
			{
				totalCount += query.queryPartitionPosttrend(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID());
			}
			
			System.out.println(String.format("There are %s posts delivered while they should not.",totalCount));
			System.out.println(String.format("Totally, there are %s posts delivered.", query.queryProjectPosttrend(35, 2008, 8)));
			query.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
