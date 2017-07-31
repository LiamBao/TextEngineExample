package com.cic.textengine.datadelivery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.common.service.database.DB;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class DataDelivery {
	
	private static Logger m_logger = Logger.getLogger(DataDelivery.class);
	
	private String nndaemonAddress = null;
	private int nndaemonPort = 0;
	
	public DataDelivery(String nnAddress, int nnPort)
	{
		this.nndaemonAddress = nnAddress;
		this.nndaemonPort = nnPort;
	}
	
	public synchronized void datadelivery(int year, int month, String source, String siteid, String forumid, int projectID) throws Exception
	{
		
		/*
		 * Query T_PARTITION_CHECKPOINT to get checkpoint 
		 */
		long checkpoint_itemid = 0;
		String source_siteid = source+siteid;
		
		DB.createConnection();
		Connection ddconn = DB.getConnection();
		Statement ddstat = ddconn.createStatement();
		String sql = String.format("select * from T_POSTTREND where site_id = '%s' and forum_id = '%s' and year = '%s' and month = '%s'",
				source_siteid, forumid, year, month);
		ddstat.execute(sql);
		ResultSet ddresult = ddstat.getResultSet();
		if(ddresult.next()) {
			checkpoint_itemid = ddresult.getLong("checkpoint_itemid");
//			checkpoint_itemid = ddresult.getLong("post_count");
			if(checkpoint_itemid == 0)
			{
				m_logger.error("The date have not been fixed.");
				throw new Exception("The date have not been fixed.");
			}
		} else {
			m_logger.error(String.format("Invalid partition key: [year:%s, month:%s, siteid:%s, forumid:%s]", year, month, source_siteid, forumid));
			throw new Exception(String.format("Invalid partition key: [year:%s, month:%s, siteid:%s, forumid:%s]", year, month, source_siteid, forumid));
		}
			
			
		/*
		 * Get all the meta info by search the IR
		 */
		
//		String queryStr = String.format("yearofpost: %s AND monthofpost: %s AND siteid: %s AND forumid: %s AND source: %s", 
//				year, month, siteid, forumid, source);
//		SearchManager manager = new SearchManager();
//		ArrayList<SearchResultBean> searchRes = manager.search(queryStr);
		
		NameNodeClient nnclient = new NameNodeClient(nndaemonAddress, nndaemonPort);
		DataNodeClient dnclient = nnclient.getDNClientForQuery(year, month, source_siteid, forumid);
		
		RemoteTEItemEnumerator enu = dnclient.getItemEnumerator(year, month, source_siteid, forumid, 0, checkpoint_itemid, false);
		ArrayList<TEItem> itemlist = new ArrayList<TEItem>();
		
		while(enu.next())
		{
			itemlist.add(enu.getItem());
		}
		/*
		 * deliver the meta data of items before the checkpoint of the partition
		 */
		ProjectDB projectdb = new ProjectDB(projectID);
//		for(int i=0; i<searchRes.size(); i++)
//		{
//			TEItemMeta meta = searchRes.get(i).getItemMeta();
//			if(meta.getItemID() <= checkpoint_itemid) {
//				projectdb.addMetaInfo(meta);
//			}
//		}
		
		for(int i=0; i<itemlist.size(); i++)
		{
			TEItemMeta meta = (TEItemMeta) itemlist.get(i).getMeta();
			if(meta.getItemID() <= checkpoint_itemid) {
				projectdb.addMetaInfo(meta);
			}
		}
		projectdb.close();
	}
}