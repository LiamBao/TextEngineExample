package com.cic.textengine.datadelivery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.common.service.database.DB;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.type.PartitionKey;

public class DataConsolidate {

	private static Logger m_logger = Logger.getLogger(DataConsolidate.class);

	private String nnAddress = null;
	private int nnPort = 0;

	public DataConsolidate(String nnAddress, int nnPort) {
		this.nnAddress = nnAddress;
		this.nnPort = nnPort;
	}

	public synchronized long consolidate(int year, int month, 
			String siteid, String forumid) throws Exception {
		/*
		 * Query for partition_id and post_count
		 */

		long postcount = 0;
		int partitionid = 0;

		long checkpoint_count = 0;
		Timestamp checkpoint_date = null;
		long checkpoint_itemid = 0;

		DB.createConnection();
		Connection conn = DB.getConnection();
		String sql = String
				.format(
						"Select * from T_POSTTREND where year = %s and month = %s and site_id = '%s' and forum_id = '%s'",
						year, month, siteid, forumid);
		Statement stat = conn.createStatement();
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		if (result.next()) {
			postcount = result.getLong("post_count");
			partitionid = result.getInt("partition_id");
			checkpoint_date = result.getTimestamp("checkpoint_time");
		} else {
			m_logger
					.info(String
							.format(
									"no such partition in post trend DB.[y:%s, m:%s, s:%s, f:%s",
									year, month, siteid, forumid));
			m_logger.info("Insert an empty record.");
			
			sql = "insert into T_POSTTREND (year, month, site_id, forum_id, post_count, " +
					"CHECKPOINT_POST_COUNT, CHECKPOINT_TIME, CHECKPOINT_ITEMID) values(?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, year);
			ps.setInt(2, month);
			ps.setString(3, siteid);
			ps.setString(4, forumid);
			ps.setLong(5, 0);
			ps.setLong(6, 0);
			ps.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
			ps.setLong(8, 0);
			ps.execute();
			// throw new Exception("No such partition in post trend DB.");
			return 0;
		}

		/*
		 * data consolidate
		 */

		if (checkpoint_date != null) {
			m_logger
					.info(String
							.format(
									"The partitoin have been consolidate.[year:%s, month:%s, siteid:%s, forumid:%s]",
									year, month, siteid, forumid));
			return 0;
		} else {

			checkpoint_date = new Timestamp(System.currentTimeMillis());
			checkpoint_count = postcount;
			if(checkpoint_count != 0)
			{
				// query item_id
				NameNodeClient nnclient = new NameNodeClient(nnAddress, nnPort);
				String dn_key = nnclient.getDNClientForQuery(year, month,
						siteid, forumid).getDNKey();
				checkpoint_itemid = nnclient.getDNPartitionItemCount(dn_key, year,
						month, siteid, forumid);
				if(checkpoint_itemid < checkpoint_count){
					throw new Exception("Something is wrong here. The item ID should not be smaller than checkpoint_post_count!");
				}
			} else {
				checkpoint_itemid = 0;
			}
			
			// data consolidate
			sql = "update T_POSTTREND set checkpoint_post_count = ?, checkpoint_time = ?, checkpoint_itemid = ? where partition_id = ?";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setLong(1, checkpoint_count);
			ps.setTimestamp(2, checkpoint_date);
			ps.setLong(3, checkpoint_itemid);
			ps.setInt(4, partitionid);
			ps.execute();
			m_logger
					.info(String
							.format(
									"Consolidate partition[year:%s, month:%s, siteid:%s, forumid:%s]: %s posts (itemid:%s)",
									year, month, siteid, forumid,
									checkpoint_count, checkpoint_itemid));
			return checkpoint_count;
		}
	}

	public synchronized void unConsolidate(int year, int month, 
			String siteid, String forumid) throws Exception {

		
		long partitionID = 0;

		DB.createConnection();
		Connection conn = DB.getConnection();

		String sql = String
				.format(
						"Select * from T_POSTTREND where year = %s and month = %s and site_id = '%s' and forum_id = '%s'",
						year, month, siteid, forumid);

		Statement stat = conn.createStatement();
		stat.execute(sql);

		ResultSet result = stat.getResultSet();
		if (result.next())
			partitionID = result.getLong("partition_id");

		sql = String
				.format(
						"update T_POSTTREND set checkpoint_post_count = null, checkpoint_time = null, checkpoint_itemid = null where partition_id = %s",
						partitionID);
		stat.execute(sql);
		m_logger
				.info(String
						.format(
								"Unconsolidate partition [year:%s, month:%s, siteid:%s, forumid:%s].",
								year, month, siteid, forumid));
	}

	public synchronized void unConsolidate(int year, int month, String siteid) throws Exception {
		
		ArrayList<PartitionKey> parkeyList = new ArrayList<PartitionKey>();
		
		DB.createConnection();
		Connection conn = DB.getConnection();
		String sql = String.format("select * from T_POSTTREND where year = %s and month = %s and site_id = '%s'", year, month, siteid);
		Statement stat = conn.createStatement();
		stat.execute(sql);
		
		ResultSet result = stat.getResultSet();
		while(result.next())
		{
			String forumid = result.getString("forum_id");
			parkeyList.add(new PartitionKey(year, month, siteid, forumid));
		}
		
		for(int i=0; i<parkeyList.size(); i++)
		{
			PartitionKey key = parkeyList.get(i);
			unConsolidate(year, month, siteid, key.getForumID());
		}
	}
	public synchronized void close() {
		DB.close();
	}
	
	public static void main(String[] args)
	{
		if(args.length < 2){
			System.out.println("At least 2 parameters needed");
			System.out.println("Usage: project id, month, [siteid, forumid]");
			return;
		}
		int projectId = Integer.parseInt(args[0]);
		String[] monthArray = args[1].split(",");
		String targetSite = null;
		String targetForum = null;
		
		if(args.length >=3){
			targetSite = args[2].trim();
		}
		if(args.length >= 4){
			targetForum = args[3].trim();
		}
		
		DataConsolidate con = new DataConsolidate("192.168.2.2", 6869);

		try {
			DcmisDB.createConnection();
			Connection conn = DcmisDB.getConnection();
			
			String sql = "SELECT * FROM T_PROJECT_FORUM where project_id = " + projectId;
			if(targetSite != null){
				sql = sql.concat(" AND site_id = '"+targetSite+" '");
			}
			if(targetForum != null){
				sql = sql.concat(" AND forum_id = '"+targetForum+" '");
			}
			Statement stat = conn.createStatement();
			stat.execute(sql);
			ResultSet rs = stat.getResultSet();
			while(rs.next()) {
				String siteid = rs.getString("site_id");
				String forumid = rs.getString("forum_id");
				for(int i=0; i<monthArray.length; i++){
					int year = Integer.parseInt(monthArray[i].substring(0, 4).trim());
					int month = Integer.parseInt(monthArray[i].substring(4).trim());
					con.unConsolidate(year, month, siteid, forumid);
				}
			}
			con.close();
			conn.close();
			System.out.println("Success to unconsolidate project:"+projectId);
		} catch (Exception e) {
			System.out.println("fail to unconsolidate project: "+projectId);
			System.out.println("Because: "+e.getLocalizedMessage());
		}
	}
}
