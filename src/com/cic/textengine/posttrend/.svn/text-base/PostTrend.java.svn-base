package com.cic.textengine.posttrend;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.cic.common.service.database.DB;
import com.cic.textengine.repository.type.PartitionKey;

public class PostTrend {
	private static Logger m_logger = Logger.getLogger(PostTrend.class);

	public synchronized void addTrend(PartitionKey parKey, int count) throws Exception {
		DB.createConnection();

		Connection conn = DB.getConnection();
		Statement stat = null;
		stat = conn.createStatement();

		int year = parKey.getYear();
		int month = parKey.getMonth();
		String siteId = parKey.getSiteID();
		String forumId = parKey.getForumID();
		String sql = String
				.format(
						"select partition_id, post_count from T_POSTTREND where year = '%s' and month='%s' and site_id='%s' and forum_id='%s'",
						year, month, siteId, forumId);
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		long partitionId = 0;
		if (result.next()) {
			partitionId = result.getLong("partition_id");
//			stat
//					.execute(String
//							.format(
//									"select post_count from T_POSTTREND where partition_id = '%s'",
//									partitionId));
//			result = stat.getResultSet();
//			if (result.next())
			
			int currentCount = 0;
			currentCount = result.getInt("post_count");
			currentCount += count;

			String updateSql = String.format(
					"update T_POSTTREND set post_count='%s' where partition_id = '%s'", currentCount, partitionId);
			stat.execute(updateSql);
			m_logger.info(updateSql);
		} else {
			/* if there is no existing partition */
			String insertSql = String
					.format(
							"insert into T_POSTTREND (year, month, site_id, forum_id, post_count) values('%s', '%s', '%s', '%s', '%s')",
							year, month, siteId, forumId, count);
			stat.execute(insertSql);
			m_logger.info(insertSql);
		}
	}
	
	public synchronized void deleteTrend(PartitionKey parKey, int count) throws Exception {
		DB.createConnection();

		Connection conn = DB.getConnection();
		Statement stat = null;
		stat = conn.createStatement();

		int year = parKey.getYear();
		int month = parKey.getMonth();
		String siteId = parKey.getSiteID();
		String forumId = parKey.getForumID();
		String sql = String
		.format(
				"select partition_id from T_POSTTREND where year = '%s' and month='%s' and site_id='%s' and forum_id='%s'",
				year, month, siteId, forumId);
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		long partitionid = 0;
		if(result.next()) {
			partitionid = result.getLong("partition_id");
			stat.execute(String.format("select post_count from T_POSTTREND where partition_id = '%s'",
									partitionid));
			result = stat.getResultSet();
			int currentCount = 0;
			if(result.next())
				currentCount = result.getInt("post_count");
			currentCount -=count;
			
			String updateSql = String.format(
					"update T_POSTTREND set post_count='%s' where partition_id = '%s'", currentCount, partitionid);
			stat.execute(updateSql);
			m_logger.info(updateSql);
		}
	}
	
	public synchronized void cleanTrend(PartitionKey parKey) throws Exception
	{
		DB.createConnection();

		Connection conn = DB.getConnection();
		Statement stat = null;
		stat = conn.createStatement();

		int year = parKey.getYear();
		int month = parKey.getMonth();
		String siteId = parKey.getSiteID();
		String forumId = parKey.getForumID();
		String sql = String
		.format(
				"select partition_id from T_POSTTREND where year = '%s' and month='%s' and site_id='%s' and forum_id='%s'",
				year, month, siteId, forumId);
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		long partitionid = 0;
		
		if(result.next())
		{
			partitionid = result.getLong("partition_id");
			String updateSql = String.format(
					"update T_POSTTREND set post_count=0 where partition_id = '%s'", partitionid);
			stat.execute(updateSql);
			m_logger.info(updateSql);
		}
	}
	
	public synchronized void setTrend(PartitionKey parKey, int count) throws Exception
	{
		DB.createConnection();
		Connection conn = DB.getConnection();
		Statement stat = conn.createStatement();
		int year = parKey.getYear();
		int month = parKey.getMonth();
		String siteid = parKey.getSiteID();
		String forumid = parKey.getForumID();
		String sql = String.format(
				"select partition_id from T_POSTTREND " +
				"where year = %s and month = %s and site_id = '%s' and forum_id = '%s'", year, month, siteid, forumid);
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		long partitionid = 0;
		if(result.next())
		{
			partitionid = result.getLong("partition_id");
			String updateSql = String.format("update T_POSTTREND set post_count = %s where partition_id = %s", count, partitionid);
			stat.execute(updateSql);
			m_logger.info(updateSql);
		}
	}
	
	public int getTrend(PartitionKey parKey) throws Exception
	{
		int count = 0;
		DB.createConnection();
		Connection conn = DB.getConnection();
		Statement stat = conn.createStatement();
		int year = parKey.getYear();
		int month = parKey.getMonth();
		String siteid = parKey.getSiteID();
		String forumid = parKey.getForumID();
		String sql = String.format(
				"select post_count from T_POSTTREND " +
				"where year = %s and month = %s and site_id = '%s' and forum_id = '%s'", year, month, siteid, forumid);
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		if(result.next())
		{
			count = result.getInt("post_count");
		}
		return count;
	}
	
	public void close()
	{
		DB.close();
	}

	public static void main(String[] args) {
		PartitionKey parKey1 = new PartitionKey(2007, 11, "5678", "1234");
		PartitionKey parKey2 = new PartitionKey(2007, 2, "5678", "1234");
		PostTrend pt = new PostTrend();
		try {
			DB.createConnection();
			for (int i = 1; i < 1000; i++) {
				pt.addTrend(parKey1, 100);
			}
			for (int i = 1; i < 100; i++) {
				pt.addTrend(parKey2, 33);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
