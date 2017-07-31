package com.cic.textengine.diagnose;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class FindErrorPartition {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		
		if(args.length < 4){
			System.out.println("4 parameters needed: IP, DB, BeginID, EndID");
			return;
		}
		String ip = args[0].trim();
		String db = args[1].trim();
		long begin = Long.parseLong(args[2].trim());
		long end = Long.parseLong(args[3].trim());
		
//		String ip = "192.168.1.22";
//		String db = "LeoProject649";
//		long begin = 16765568L;
//		long end =17217466L;
//		ArrayList<String> itemRange = new ArrayList<String>();
//		itemRange.add("290907027_291007026");
//		itemRange.add("302007027_302107026");
//		itemRange.add("302907027_303007026");
//		itemRange.add("304715452_304815451");
//		itemRange.add("304815452_304915451");
//		itemRange.add("304915452_305015451");
//		itemRange.add("305015452_305115451");
//		itemRange.add("305415452_305515451");
//		itemRange.add("305515452_305615451");
//		itemRange.add("305815452_305915451");
//		itemRange.add("305915452_306015451");
//		itemRange.add("306015452_306115451");
//		itemRange.add("306215452_306315451");
//		itemRange.add("306415452_306515451");
//		itemRange.add("306515452_306615451");
//		itemRange.add("313115452_313215451");
//		itemRange.add("313215452_313315451");
//		itemRange.add("313315452_313415451");
//		itemRange.add("313415452_313515451");
//		itemRange.add("313515452_313615451");
//		itemRange.add("313615452_313715451");
//		itemRange.add("313915452_314015451");
//		itemRange.add("315915452_316015451");
//		itemRange.add("316015452_316115451");
//		itemRange.add("316115452_316215451");
//		itemRange.add("316315452_316415451");
//		itemRange.add("317815452_317915451");
//		itemRange.add("317915452_318015451");
//		itemRange.add("318015452_318115451");
//		itemRange.add("318115452_318215451");
//
//		for(String range: itemRange){
//			long begin = Long.parseLong(range.split("_")[0]);
//			long end = Long.parseLong(range.split("_")[1]);
			// load item key in the data range
			Class.forName("com.mysql.jdbc.Driver");
			String url = String.format("jdbc:mysql://%s:3306/%s?characterEncoding=utf8", ip, db);
			String user = "leo";
			String passwd = "cicdata";
			Connection conn = DriverManager.getConnection(url, user, passwd);
			Statement st = conn.createStatement();
			
			String sql = String.format("SELECT ITEM_KEY FROM DS_ITEM WHERE ITEM_ID >= %s AND ITEM_ID <= %s", begin, end);
			st.execute(sql);
			ResultSet rs = st.getResultSet();
			ArrayList<String> itemlist = new ArrayList<String>();
			while(rs.next()){
				itemlist.add(rs.getString("ITEM_KEY"));
			}
			conn.close();
			System.out.println(String.format("%s items read.", itemlist.size()));
			int count = 0;
			HashSet<PartitionKey> parkeylist = new HashSet<PartitionKey>();
			TEClient client = new TEClient("192.168.2.2", 6869);
			// for each item key, get the data and log the item key that failed
			for(String itemkey : itemlist){
				try {
					TEItem item = client.getItem(itemkey);
//					System.err.println(item.getSubject());
				} catch (Exception e) {
					System.err.println(itemkey);
					try {
						ItemKey ik = ItemKey.decodeKey(itemkey);
						PartitionKey parkey = PartitionKey.decodeStringKey(ik.getPartitionKey());
						parkeylist.add(parkey);
					} catch (DecoderException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				count ++;
				if (count % 500 == 0)
					System.out.println(String.format("%s items handled.", count));
			}
			// print the failed partition key 
			for(PartitionKey pk : parkeylist){
				System.out.println(pk.generateStringKey());
			}
//		}
	}

}
