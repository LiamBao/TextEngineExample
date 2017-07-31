package com.cic.textengine.diagnose;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.apache.commons.codec.DecoderException;

import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import com.cic.textengine.repository.importer.JDBMManager;
import com.cic.textengine.repository.type.PartitionKey;

public class PrintPartitionInfo {

	/**
	 * these codes are written for the TM data import.
	 * it inserts the partition information into the T_PARTITION table.
	 */
	
	public static String PARTITION_KEY= "PatitionRecord";
	String dbPath = "ItemImporterDB_08";
	private JDBMManager jdbmMan = null;
	private BTree parKeyBTree = null;
	private ArrayList<Partition> parList = null;
	
	private ArrayList<Long> partitionIDList = null;
	
	private String url = "jdbc:mysql://192.168.2.2:3306/TENameNodeRepository";
	private String url_posttrend = "jdbc:mysql://192.168.2.2:3306/TEPostTrend";
	private String user = "TENN002";
	private String pwd = "Vj3tRws2";
	private String dnkey1 = "18bd6200-7c91-41ff-abda-070da4442826";
	private String dnkey2 = "0c9d9c1d-93a7-49e7-92a3-99cae15de3e9";
	
	public PrintPartitionInfo() {
		parList = new ArrayList<Partition>();
		try {
			jdbmMan = JDBMManager.getInstance(dbPath);
			parKeyBTree = jdbmMan.getBTree(PARTITION_KEY);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void update() {
		try {
			long timeStamp = System.currentTimeMillis();
			TupleBrowser br = parKeyBTree.browse();
			System.out.println(String.format("Totally there are %s partitions.", parKeyBTree.size()));
			Tuple tuple = new Tuple();
			while(br.getNext(tuple)) {
				String key = tuple.getKey().toString();
				Long itemCount = (Long) (tuple.getValue());
				parList.add(new Partition(key, itemCount, timeStamp));
			}
			System.out.println(String.format("Finish get all the partition info.(totally, %s partitions)", parList.size()));
			jdbmMan.close();
			
//			updatePartition();
			
//			updateDNLog();
			
			updatePostTrend();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void updatePartition() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, user, pwd);
			PreparedStatement ps = conn.prepareStatement("INSERT INTO T_PARTITION (THE_YEAR, THE_MONTH, SITE_ID, FORUM_ID, CREATE_DT, VERSION, TS_LAST_MODIFY, ITEM_COUNT) values (?, ?, ?, ?, ?, ?, ?, ?)");
			int count = 0;
			
			for(Partition par: parList) {
				ps.setInt(1, par.getYear());
				ps.setInt(2, par.getMonth());
				ps.setString(3, par.getSiteid());
				ps.setString(4, par.getForumid());
				ps.setTimestamp(5, new Timestamp(par.getTimeStamp()));
				ps.setInt(6, par.getVersion());
				ps.setTimestamp(7, new Timestamp(par.getTimeStamp()));
				ps.setLong(8, par.getItemCount());

				ps.execute();
				
				count ++;
				if(count %5000 == 0) {
					System.out.println(String.format("%s partitions have been added.", count));
				}
			}
			
			System.out.println("Finish add partitions......");
			
			ps.close();
			conn.close();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void updateDNLog() {

		ArrayList<String> dnkeyList = new ArrayList<String>();
		dnkeyList.add(dnkey1);
		dnkeyList.add(dnkey2);
		int count = 0;
		
		try {
			String dnSql = "insert into T_DN_PARTITION (DN_KEY, PARTITION_ID, PARTITION_ITEM_COUNT, TS_LAST_MODIFY, VERSION) values (?, ?, ?, ?, ?)";
			String logSql = "insert into T_PARTITION_OPERATION_LOG (PARTITION_ID, VERSION, OPERATION_TYPE, OPERATION_DATA, TS) values (?, ?, ?, ?, ?)";
			String parSql = "select * from T_PARTITION where the_year = ? and the_month = ? and site_id = ? and forum_id = ?";
			
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, user, pwd);
			PreparedStatement psDn = conn.prepareStatement(dnSql);
			PreparedStatement psLog = conn.prepareStatement(logSql);
			PreparedStatement ps = conn.prepareStatement(parSql);
			
			long partitionID = 0;
			
			for (Partition par : parList) {
				
				byte[] buff = getData(1, par.getItemCount(), dnkeyList);
				
				// query the partition ID
				ps.setInt(1, par.getYear());
				ps.setInt(2, par.getMonth());
				ps.setString(3, par.getSiteid());
				ps.setString(4, par.getForumid());
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					partitionID = rs.getLong("PARTITION_ID");
				} else {
					throw new Exception(
							String
									.format(
											"Fail to find the partition[year: %s, month: %s, siteid: %s, forumid: %s].",
											par.getYear(), par.getMonth(), par
													.getSiteid(), par
													.getForumid()));
				}
				
				for (String dnkey : dnkeyList) {
					psDn.setString(1, dnkey);
					psDn.setLong(2, partitionID);
					psDn.setLong(3, par.getItemCount());
					psDn.setTimestamp(4, new Timestamp(par.getTimeStamp()));
					psDn.setInt(5, par.getVersion());
					psDn.execute();
				}
				
				psLog.setLong(1, partitionID);
				psLog.setInt(2, par.getVersion());
				psLog.setInt(3, par.getOperationType());
				psLog.setBinaryStream(4, new ByteArrayInputStream(buff), buff.length);
				psLog.setTimestamp(5, new Timestamp(par.getTimeStamp()));
				psLog.execute();
				
				count ++;
				if(count %5000 == 0) {
					System.out.println(String.format("%s partitions info have been added.", count));
				}
			}
			
			System.out.println("Finish add partition info......");
			ps.close();
			psDn.close();
			psLog.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void updatePostTrend() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url_posttrend, user, pwd);
			String sql = "insert into T_POSTTREND (year, month, site_id, forum_id, post_count, checkpoint_post_count, checkpoint_time, checkpoint_item_id) values (?,?,?,?,?,?,?,?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			int count = 0;
			for(Partition partition: parList) {
				ps.setInt(1, partition.getYear());
				ps.setInt(2, partition.getMonth());
				ps.setString(3, partition.getSiteid());
				ps.setString(4, partition.getForumid());
				ps.setLong(5, partition.getItemCount());
				ps.setLong(6, partition.getItemCount());
				ps.setTimestamp(7, new Timestamp(partition.getTimeStamp()));
				ps.setLong(8, partition.getItemCount());
				ps.execute();
				count ++;
				if(count % 5000 == 0) {
					System.out.println(String.format("%s post trend info have been added.", count));
				}
			}
			
			System.out.println("Finish add post trend info......");
			conn.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void clean() {
		partitionIDList = new ArrayList<Long>();
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, user, pwd);
			String sql = "select PARTITION_ID from T_PARTITION where site_id like 'TM%'";
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sql);
			while(rs.next()) {
				long partitionID = rs.getLong("PARTITION_ID");
				partitionIDList.add(partitionID);
			}
			conn.close();
			System.out.println(String.format("Totally, there are %s partitions.", partitionIDList.size()));
			
			cleanDNLog();
			
			cleanPartition();
			
			cleanPostTrend();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void cleanDNLog() {
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, user, pwd);
			String sql_dn = "delete from T_DN_PARTITION where partition_id = ?";
			PreparedStatement ps_dn = conn.prepareStatement(sql_dn);
			
			String sql_log = "delete from T_PARTITION_OPERATION_LOG where partition_id = ?";
			PreparedStatement ps_log = conn.prepareStatement(sql_log);
			
			int count = 0;
			
			for(long partitionID : partitionIDList) {
				ps_dn.setLong(1, partitionID);
				ps_dn.execute();
				
				ps_log.setLong(1, partitionID);
				ps_log.execute();
				
				count ++;
				if(count % 5000 == 0) {
					System.out.println(String.format("%s partition info have been deleted.", count));
				}
			}
			System.out.println(String.format("Totally, there are %s partitions info deleted.", count));
			ps_dn.close();
			ps_log.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void cleanPartition() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, user, pwd);
			String sql = "delete from T_PARTITION where partition_id = ?";
			PreparedStatement ps = conn.prepareStatement(sql);
						
			int count = 0;
			
			for(long partitionID : partitionIDList) {
				ps.setLong(1, partitionID);
				ps.execute();
				
				count ++;
				if(count % 5000 == 0) {
					System.out.println(String.format("%s partitions have been deleted.", count));
				}
			}
			System.out.println(String.format("Totally, there are %s partitions deleted.", count));
			ps.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void cleanPostTrend() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url_posttrend, user, pwd);
			String sql = "delete from T_POSTTREND where site_id like 'TM%'";
			Statement stat = conn.createStatement();
			stat.execute(sql);
			System.out.println("Finish clean the post trend databases.");
			conn.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static byte[] getData(long startItemID, long itemCount, ArrayList<String> dnkeyList) throws IOException {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeLong(startItemID);
		dos.writeLong(itemCount);
		dos.writeInt(dnkeyList.size());
		for (String dnkey: dnkeyList){
			dos.writeUTF(dnkey);
		}
		
		byte[] res = baos.toByteArray();
		dos.close();
		return res;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PrintPartitionInfo printInfo = new PrintPartitionInfo();
		printInfo.update();
	}
	
	public class Partition {
		
		int year = 0;
		int month = 0;
		String siteid = "";
		String forumid = "";
		long itemCount = 0;
		int version = 1;
		int operationType = 1;
		long timeStamp = 0;
		
		public Partition(String parkey, long itemCount, long timeStamp) throws DecoderException {
			PartitionKey key = PartitionKey.decodeStringKey(parkey);
			setYear(key.getYear());
			setMonth(key.getMonth());
			setSiteid(key.getSiteID());
			setForumid(key.getForumID());
			setItemCount(itemCount);
			setTimeStamp(timeStamp);
		}
		
		public int getYear() {
			return year;
		}
		public void setYear(int year) {
			this.year = year;
		}
		public int getMonth() {
			return month;
		}
		public void setMonth(int month) {
			this.month = month;
		}
		public String getSiteid() {
			return siteid;
		}
		public void setSiteid(String siteid) {
			this.siteid = siteid;
		}
		public String getForumid() {
			return forumid;
		}
		public void setForumid(String forumid) {
			this.forumid = forumid;
		}
		public long getItemCount() {
			return itemCount;
		}
		public void setItemCount(long itemCount) {
			this.itemCount = itemCount;
		}
		public int getVersion() {
			return version;
		}
		public void setVersion(int version) {
			this.version = version;
		}
		public int getOperationType() {
			return operationType;
		}
		public void setOperationType(int operationType) {
			this.operationType = operationType;
		}
		public long getTimeStamp() {
			return timeStamp;
		}
		public void setTimeStamp(long timeStamp) {
			this.timeStamp = timeStamp;
		}
	}

}
