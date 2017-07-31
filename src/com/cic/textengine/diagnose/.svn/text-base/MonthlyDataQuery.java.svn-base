package com.cic.textengine.diagnose;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

import com.cic.common.service.database.DB;
import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.TEItemEnumerator;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class MonthlyDataQuery {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// Load all the FF forums of Apr. 2013
		DB.createConnection();
		Connection conn = DB.getConnection();
		Statement stat = null;
		stat = conn.createStatement();
		String sql = "SELECT YEAR, MONTH, SITE_ID, FORUM_ID, POST_COUNT FROM T_POSTTREND WHERE YEAR = 2013 AND MONTH = 3";
		stat.execute(sql);
		
		ArrayList<PartitionKey> parkeyList = new ArrayList<PartitionKey>();
		long totalCount = 0L;
		ResultSet rs = stat.getResultSet();
		while(rs.next()){
			String siteid = rs.getString("SITE_ID");
			String forumid = rs.getString("FORUM_ID");
			int count = rs.getInt("POST_COUNT");
			if(siteid.equals("FF1094") || siteid.equals("FF1225") || siteid.equals("FF1148") || count <=0 || siteid.startsWith("SERA"))
				continue;
			totalCount += count;
			parkeyList.add(new PartitionKey(2013, 3, siteid, forumid));
		}
		DB.close();
		System.out.println(String.format("Totally there are %s items in %s partitions to check.", totalCount, parkeyList.size()));
		
		// Load Nestle Keywords
		String[] kws = {"雀巢", "能恩", "力多精", "嘉宝", "鹰唛", "植脂淡奶", "惠氏", 
				"雀巢咖啡", "咖啡伴侣", "美禄", "脆脆鲨", "雀巢威化", "趣满果", "五羊", "羊少", 
				"笨nana", "笨娜娜", "八次方", "优活", "脆谷乐", "银鹭", "徐福记", "熊博士", 
				"宠优", "普瑞纳", "豪吉", "太太乐", "奈斯派索", "nespresso", "美极土豆泥", 
				"美极鲜", "美极汤", "蒙牛", "中粮", "伊利", "光明乳业", "光明牛奶", "可口可乐", "联合利华", "卡夫", "花心筒"};
		
		// Load all the data and match the keywords and print the number of topics/replies in each forum
		long tmpCount = 0L;
		PrintWriter pw = new PrintWriter(new FileWriter("/home/te_opr/TextEngine/newstleKW.csv"));
		TEClient client = new TEClient("192.168.2.2", 6869);
		for(PartitionKey pk : parkeyList){
			try{
				TEItemEnumerator enu = client.getItemEnumerator(pk.generateStringKey());
				while(enu.next()){
					TEItem item = enu.getItem();
					String text = item.getSubject()+" "+item.getContent();
					// check each kw
					for(String kw : kws){
						if(text.contains(kw)){
							pw.println(String.format("%s, %s, %s", kw, pk.generateStringKey(), item.getItemKey()));
						}
					}
					tmpCount ++;
					if(tmpCount % 10000 == 0){
						System.out.println(String.format("finish %s items.", tmpCount));
						pw.flush();
					}
				}
			} catch (Exception e){
				System.out.println("Error ( "+pk.generateStringKey()+" ): "+e.getLocalizedMessage());
			}
		}
		pw.close();
	}

}
