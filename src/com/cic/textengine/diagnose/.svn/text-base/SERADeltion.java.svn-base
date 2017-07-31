package com.cic.textengine.diagnose;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.cic.textengine.datadelivery.DataConsolidate;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.type.PartitionKey;

public class SERADeltion {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// query the partitions
		int year = 2009;
		int month = 0;
		String source = "SERA";
		String siteid = "";
		String forumid = "203";
		ArrayList<PartitionKey> parkeyList = new ArrayList<PartitionKey>();
		
		String url = "jdbc:mysql://192.168.2.2:3306/TEPostTrend";
		String user = "TENN002R";
		String password = "cictech_rnd_te";
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection(url, user, password);
		
		String sql = "select * from T_POSTTREND where year = 2009 and month in (1, 2, 3) and forum_id  = '203' and site_id like 'SERA%'";
		Statement st = conn.createStatement();
		st.execute(sql);
		ResultSet rs = st.getResultSet();
		while(rs.next()) {
			month = rs.getInt("month");
			siteid = rs.getString("site_id");
			parkeyList.add(new PartitionKey(year, month, siteid, forumid));
			System.out.println(String.format("Partition [Y:%s, M:%s, S:%s, F:%s] added.", year, month, siteid, forumid));
		}
		conn.close();
		conn = null;
		st = null;
		rs = null;
		
		// insert into the T_DELETION
		url = "jdbc:mysql://192.168.1.15:3306/DCMIS";
		user = "dcmis_opr";
		password = "knr854";
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection(url, user, password);
		String date = "2009-03-27 17:30:00";
		st = conn.createStatement();
		for(PartitionKey parkey: parkeyList) {
			sql = String
			.format(
					"insert into T_DELETION (source, site_id, forum_id, year, month, submit_time, start_time, finish_time) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
					source, parkey.getSiteID().substring(4), parkey.getForumID(), parkey.getYear(), parkey.getMonth(), date, date, date);
			System.out.println(sql);
			st.execute(sql);
		}
		conn.close();
	}

}
