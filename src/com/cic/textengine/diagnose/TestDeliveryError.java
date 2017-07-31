package com.cic.textengine.diagnose;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.TEItemEnumerator;
import com.cic.textengine.datadelivery.DcmisDB;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class TestDeliveryError {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		if(args.length< 3){
			System.out.println(String.format("Usgae: 3 parameters needed, projectid, year, month"));
			return;
		}
		
		int projectId = Integer.parseInt(args[0].trim());
		int year = Integer.parseInt(args[1].trim());
		int month = Integer.parseInt(args[2].trim());
		ArrayList<PartitionKey> parkeyList = new ArrayList<PartitionKey>();
		DcmisDB.createConnection();
		Connection conn = DcmisDB.getConnection();
		String sql = String.format("SELECT SITE_ID, FORUM_ID, POST_COUNT FROM T_PROJECT_FORUM_POSTTREND WHERE PROJECT_ID = %s AND YEAR = %s AND MONTH = %s", projectId, year, month);
		Statement st = conn.createStatement();
		st.execute(sql);
		ResultSet rs = st.getResultSet();
		int totalCount = 0;
		while(rs.next()){
			int count = rs.getInt(3);
			if(count <= 0)
				continue;
			String siteid = rs.getString(1);
			String forumid = rs.getString(2);
			PartitionKey parkey = new PartitionKey(year, month, siteid,forumid);
			parkeyList.add(parkey);
			totalCount +=count;
		}
		st.execute(sql);
		DcmisDB.close();
		System.out.println(String.format("Totally there are %s partitions, %s items.", parkeyList.size(), totalCount));
		
		TEClient client = new TEClient("192.168.2.2", 6869);
		int count = 0;
		for(PartitionKey key : parkeyList){
			try{
				TEItemEnumerator enu = client.getItemEnumerator(key.generateStringKey());
				while(enu.next()){
					TEItem item = enu.getItem();
					count ++;
					if (count % 500 == 0)
						System.out.println(count);
				}
			} catch(Exception e){
				System.out.println("Error Key: " + key.generateStringKey());
			}
		}
		client.close();

	}

}
