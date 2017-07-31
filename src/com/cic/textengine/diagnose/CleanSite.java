package com.cic.textengine.diagnose;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.common.service.database.DB;
import com.cic.textengine.repository.ItemImporter;
import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.utils.BloomFilterHelper;

public class CleanSite {

	private String nnAddr = null;
	private int nnPort = 0;
	private int year = 0;
	private int month = 0;
	private String siteid = null;
	private ArrayList<String> forumidList = null;

	private static Logger logger = Logger.getLogger(CleanSite.class);

	public CleanSite(String address, int port, int year, int month,
			String siteid) throws Exception {
		this.nnAddr = address;
		this.nnPort = port;
		this.year = year;
		this.month = month;
		this.siteid = siteid;
		this.forumidList = new ArrayList<String>();
		try {
			DB.createConnection();
			Connection conn = DB.getConnection();
			Statement stat = conn.createStatement();
			String sql = String
					.format(
							"select * from T_POSTTREND where site_id = '%s' and year = '%s' and month = '%s'",
							siteid, year, month);
			stat.execute(sql);
			ResultSet result = stat.getResultSet();
			while(result.next())
			{
				String forumid = result.getString("forum_id");
				forumidList.add(forumid);
			}

		} catch (Exception e) {
			logger.error("Error in query the post trend db for partitions.");
			throw e;
		}
	}

	public void clean() {

		for(int i=0; i<forumidList.size(); i++)
		{
			CleanPartition cp = new CleanPartition(nnAddr, nnPort, year, month, siteid, forumidList.get(i));
			cp.clean();
		}
	}

	public static void main(String args[])
	{
		if(args.length < 5)
		{
			System.out.println("5 parameters needed: NameNodeAddr NameNodePort year month siteid");
			System.exit(0);
		}
		try {
			Configurer.config(ItemImporter.ITEM_IMPORTER_PROPERTIES);
		} catch (IOException e1) {
			logger.error("Error in load the configuration.");
			return;
		}
		String nnAddr = args[0];
		int nnPort = Integer.parseInt(args[1]);
		int year = Integer.parseInt(args[2]);
		int month = Integer.parseInt(args[3]);
		String siteidList = args[4];
		String[] siteidArray = siteidList.split("_");
		
		for(int i=0; i<siteidArray.length; i++)
		{
			String siteid = siteidArray[i];
			try {
				CleanSite cs = new CleanSite(nnAddr, nnPort, year, month, siteid);
				cs.clean();
			} catch (Exception e) {
				logger.error("Error in init because:"+e.getLocalizedMessage());
			}
		}
		
		try {
			BloomFilterHelper.close();
		} catch (Exception e) {
			logger.error("Error in close bloom filter.");
		}
	}
}
