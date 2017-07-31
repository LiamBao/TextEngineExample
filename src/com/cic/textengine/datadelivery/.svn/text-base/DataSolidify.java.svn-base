package com.cic.textengine.datadelivery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.cic.common.service.database.DB;
import com.cic.textengine.repository.namenode.client.NameNodeClient;



public class DataSolidify {

	private static Logger m_logger = Logger.getLogger(DataSolidify.class);
	
	private String nnAddress = null;
	private int nnPort = 0;
	
	public DataSolidify(String nnAddress, int nnPort)
	{
		this.nnAddress = nnAddress;
		this.nnPort = nnPort;
	}
	
	public synchronized void solidify(int year, int month, String source, String siteid, String forumid) throws Exception
	{
		/*
		 * Query for partition_id and post_count
		 */
		String siteid_source = source+siteid;
		
		long postcount = 0;
		int partitionid = 0;
		
		long checkpoint_count = 0;
		Timestamp checkpoint_date = null;
		long checkpoint_itemid = 0;
		
		DB.createConnection();
		Connection conn = DB.getConnection();
		String sql = String.format("Select * from T_POSTTREND where year = %s and month = %s and site_id = '%s' and forum_id = '%s'", 
				year, month, siteid_source, forumid);
		Statement stat = conn.createStatement();
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		if(result.next())
		{
			postcount = result.getLong("post_count");
			partitionid = result.getInt("partition_id");
			checkpoint_date = result.getTimestamp("checkpoint_time");
		} else {
			m_logger.error("Error: no such partition in post trend DB.");
			throw new Exception("No such partition in post trend DB.");
		}
		
		/*
		 * data solidify
		 */
		
		if(checkpoint_date != null) {
			m_logger.error("The data have been solidified.");
		} else {
			
			checkpoint_count = postcount;
			checkpoint_date = new Timestamp(System.currentTimeMillis());
			//query item_id
//			SearchManager manager = SearchManager.getSearchManager();
//			manager.reopen();
//			String queryStr = String.format("yearofpost:%s AND monthofpost:%s AND source:%s AND siteid:%s AND forumid:%s", 
//					year, month, source, siteid, forumid);
//			ArrayList<SearchResultBean> searchresult = manager.search(queryStr);
//			for(int i=0; i<searchresult.size(); i++)
//			{
//				long tempid = searchresult.get(i).getItemMeta().getItemID();
//				if(tempid > checkpoint_itemid)
//					checkpoint_itemid = tempid;
//			}
			NameNodeClient nnclient = new NameNodeClient(nnAddress, nnPort);
			String dn_key = nnclient.getDNClientForQuery(year, month, siteid_source, forumid).getDNKey();
			checkpoint_itemid = nnclient.getDNPartitionItemCount(dn_key, year, month, siteid_source, forumid);
			// data solidify
			sql = "update T_POSTTREND set checkpoint_post_count = ?, checkpoint_time = ?, checkpoint_itemid = ? where partition_id = ?";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setLong(1, checkpoint_count);
			ps.setTimestamp(2, checkpoint_date);
			ps.setLong(3, checkpoint_itemid);
			ps.setInt(4, partitionid);
			ps.execute();
		}

		

		
	}
}
