package com.cic.textengine.diagnose;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.TEItemEnumerator;
import com.cic.textengine.client.exception.TEItemEnumeratorException;
import com.cic.textengine.repository.type.PartitionKey;

public class FixPostTrend {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * @throws TEItemEnumeratorException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, TEItemEnumeratorException {

		ArrayList<PartitionKey> parList = new ArrayList<PartitionKey>();
		HashMap<PartitionKey, Integer> par2Update = new HashMap<PartitionKey, Integer>();
		HashMap<PartitionKey, Integer> par2Insert = new HashMap<PartitionKey, Integer>();
		
		// get all the partitions from T_PARTITION where partition last modified time is after the SVN check in.
		String url = "jdbc:mysql://192.168.2.2:3306/TENameNodeRepository";
		String user = "TENN002";
		String password = "Vj3tRws2";
		
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection(url, user, password);
		
		String sql = "select * from T_PARTITION where TS_LAST_MODIFY > '2012-02-25 12:00:00'";
		Statement st = conn.createStatement();
		st.execute(sql);
		ResultSet rs = st.getResultSet();
		while(rs.next()){
			int year = rs.getInt("the_year");
			int month = rs.getInt("the_month");
			String siteid = rs.getString("site_id");
			String forumid = rs.getString("forum_id");
			
			PartitionKey parkey = new PartitionKey(year, month, siteid, forumid);
			parList.add(parkey);
		}
		
		conn.close();
		
		// update the partition post count
		url = "jdbc:mysql://192.168.2.2:3306/TEPostTrend";
		conn = DriverManager.getConnection(url, user, password);
		st = conn.createStatement();
		
		TEClient client = new TEClient("192.168.2.2", 6869);
		TEItemEnumerator enu = null;
		int parCount = 0;
		
		for (PartitionKey parkey : parList) {
			
			// get the item count in TE partition
			int count = 0;
			enu = client.getItemEnumerator(parkey.generateStringKey());
			while(enu.next()){
				count ++;
			}
			
			sql = String
			.format("select * from T_POSTTREND where year = '%s' and month = '%s' and site_id = '%s' and forum_id = '%s'",
					parkey.getYear(), parkey.getMonth(),
					parkey.getSiteID(), parkey.getForumID());
			st.execute(sql);
			rs = st.getResultSet();
			
			if(rs.next()){
				// get the post trend in DB
				int postTrend = rs.getInt("post_count");
				if(postTrend != count){
					System.out.println("Item count do not match.");
					par2Update.put(parkey, count);
				}
				
			} else {
				System.out.println("Empty partition, need insert one.");
				par2Insert.put(parkey, count);
			}
			
			parCount ++;
			if(parCount % 50 == 0){
				System.out.println(String.format("%s partitions have been checked.", parCount));
			}
		}
		
		System.out.println("Begin update operation..."+par2Update.size());
		for (PartitionKey parkey : par2Update.keySet()) {
			sql = String
					.format("update T_POSTTREND set POST_COUNT='%s' where year = '%s' and month = '%s' and site_id = '%s' and forum_id = '%s'",
							par2Update.get(parkey),parkey.getYear(), parkey.getMonth(),
							parkey.getSiteID(), parkey.getForumID()
							);
			System.out.println(sql);
			st.execute(sql);
		}
		
		System.out.println("Begin insert operation..."+par2Insert.size());
		for (PartitionKey parkey : par2Insert.keySet()) {
			sql = String
					.format("insert into T_POSTTREND (YEAR, MONTH, SITE_ID, FORUM_ID, POST_COUNT) values ('%s','%s','%s','%s','%s')",
							parkey.getYear(), parkey.getMonth(),
							parkey.getSiteID(), parkey.getForumID(),
							par2Update.get(parkey));
			System.out.println(sql);
			st.execute(sql);
		}
		conn.close();
		System.out.println("end operation");

	}

}
