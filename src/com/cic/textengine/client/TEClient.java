package com.cic.textengine.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;
import org.apache.log4j.Logger;

import com.cic.textengine.client.exception.TEClientException;
import com.cic.textengine.client.exception.TEItemEnumeratorException;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.repository.PartitionSearcher;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class TEClient {
	
	private static Logger logger = Logger.getLogger(TEClient.class);
	
	private String nndaemonAddr = null;
	private int nndaemonPort = 0;
	private NameNodeClient nn_client = null;
	
	private HashMap<String, ArrayList<String>> partitionClientAddrMap = null;		// partition和DataNode的关联
	private HashMap<String, DataNodeClient> clientAddrMap = null;		// DN列表：dn address和dn client
	private HashMap<String, Integer> clientReadCountMap = null;			// DN 读取次数
	private HashMap<String, Float> clientResponseTime = null;			// DN 响应时间
	private ArrayList<String> disabledList = null;						// 禁止读取数据的DN列表

	public TEClient(String host, int port)
	{
		this.nndaemonAddr = host;
		this.nndaemonPort = port;
		this.nn_client = new NameNodeClient(this.nndaemonAddr, this.nndaemonPort);
		this.partitionClientAddrMap = new HashMap<String, ArrayList<String>>();
		this.clientAddrMap = new HashMap<String, DataNodeClient>();
		this.clientReadCountMap = new HashMap<String, Integer>();
		this.clientResponseTime = new HashMap<String, Float>();
		this.disabledList = new ArrayList<String>();
	}

	public ArrayList<TEItem> getItemsByItemkey(ArrayList<String> itemkeys) throws TEClientException
	{
		ArrayList<TEItem> itemlist = new ArrayList<TEItem>();
		for(String key: itemkeys) {
			TEItem item = getItem(key);
			itemlist.add(item);
		}
		return itemlist;
	}
	
	public synchronized TEItem getItem(String itemkey, String dnHost, int port) throws TEClientException
	{
		ItemKey itemKey = null;
		TEItem item = null;
		String dnAddress = dnHost+port;
		try {
			itemKey = ItemKey.decodeKey(itemkey);
		} catch (DecoderException e2) {
			logger.error("Invalid item key: "+itemkey);
			throw new TEClientException(e2);
		}
		
		if(!clientAddrMap.containsKey(dnAddress))
		{
			DataNodeClient dn_client = new DataNodeClient(dnHost, port);
			clientAddrMap.put(dnAddress, dn_client);
		}
		DataNodeClient dn_client = clientAddrMap.get(dnAddress);
		try {
			item = dn_client.queryItem(itemKey.getYear(), itemKey.getMonth(), itemKey.getSource()+itemKey.getSiteID(), itemKey.getForumID(),itemKey.getItemID());
			return item;
		} catch (DataNodeClientCommunicationException e) {
			logger.error("Error: data node communication exception:"+e.getLocalizedMessage());
			throw new TEClientException(e);
		} catch (DataNodeClientException e) {
			logger.error("Error: data node client exception:"+e.getLocalizedMessage());
			throw new TEClientException(e);
		}
	}
	
	public synchronized TEItem getItem(String itemkey) throws TEClientException
	{
		ItemKey itemKey = null;
		String parKey = null;
		TEItem item = null;
		ArrayList<String> dnList = null;
		try {
			itemKey = ItemKey.decodeKey(itemkey);
		} catch (DecoderException e2) {
			logger.error("Invalid item key: "+itemkey);
			throw new TEClientException(e2);
		}
		
		parKey = itemKey.getPartitionKey();
		if(!partitionClientAddrMap.containsKey(parKey))
		{
			try{
				dnList = nn_client.getDNListForQuery(itemKey.getYear(), itemKey.getMonth(), itemKey.getSource()+itemKey.getSiteID(), itemKey.getForumID());
			}catch (NameNodeClientException e) {
				logger.error("Fail to talk to name node, sleep 1 min and try again.");
				try {
					Thread.sleep(1000*60);
				} catch (InterruptedException e1) {
					// ignore
				}
				try {
					dnList = nn_client.getDNListForQuery(itemKey.getYear(), itemKey.getMonth(), itemKey.getSource()+itemKey.getSiteID(), itemKey.getForumID());
				} catch (NameNodeClientException e1) {
					logger.error("Fail to talk to name node again, exit.");
					throw new TEClientException(e1);
				}
			}
			
//			// if 2.116 is in the list remove other DN for test
//			boolean found116 = false;
//			for(String dnAddress: dnList){
//				String[] str_array = dnAddress.split(":"); 
//				String address = str_array[0].trim();
//				if(address.equals("192.168.2.116")){
//					found116 = true;
//				}
//			}
//			if(found116){
//				dnList = new ArrayList<String>();
//				dnList.add("192.168.2.116:6767");
//			}
			
			
			partitionClientAddrMap.put(parKey, dnList);
			
			for(String dnAddress: dnList)
			{
				if(!clientAddrMap.containsKey(dnAddress))
				{
					String[] str_array = dnAddress.split(":"); 
					String address = str_array[0].trim();
					int port = Integer.parseInt(str_array[1].trim());
					DataNodeClient dn_client = new DataNodeClient(address, port);
					clientAddrMap.put(dnAddress, dn_client);
					clientReadCountMap.put(dnAddress, 0);
					clientResponseTime.put(dnAddress, 0f);
				}
			}
		}
		
		String dnAddress = getBetterDN(partitionClientAddrMap.get(parKey));
		
		try {
			DataNodeClient dn_client = clientAddrMap.get(dnAddress);
			long ts = System.currentTimeMillis();
			item = dn_client.queryItem(itemKey.getYear(), itemKey.getMonth(), itemKey.getSource()+itemKey.getSiteID(), itemKey.getForumID(),itemKey.getItemID());
			logDNResponseTimeAndCount(dnAddress, System.currentTimeMillis() - ts);
			return item;
			
		}  catch (DataNodeClientCommunicationException e) {
			
			try {
				// switch to another DN and try once more
				logger.error("Error: data node communication exception:"+e.getLocalizedMessage());
				logger.debug("Disable the DN and switch to another DN, if there is.");
				disableDN(dnAddress);
				dnAddress = getBetterDN(partitionClientAddrMap.get(parKey));
				if(dnAddress != null){
					DataNodeClient dn_client = clientAddrMap.get(dnAddress);
					long ts = System.currentTimeMillis();
					item = dn_client.queryItem(itemKey.getYear(), itemKey.getMonth(), itemKey.getSource()+itemKey.getSiteID(), itemKey.getForumID(),itemKey.getItemID());
					logDNResponseTimeAndCount(dnAddress, System.currentTimeMillis() - ts);
					return item;
				} else{
					logger.error("Fail to get data for Item Key: "+itemkey);
					throw new TEClientException("No active Data Node available.");
				}
			} catch (DataNodeClientCommunicationException e1) {
				logger.error("Fail to get data for Item Key: "+itemkey);
				logger.error("Error: data node communication exception:"+e.getLocalizedMessage());
				throw new TEClientException(e1);
			} catch (DataNodeClientException e1) {
				logger.error("Fail to get data for Item Key: "+itemkey);
				logger.error("Error: data node communication exception:"+e.getLocalizedMessage());
				throw new TEClientException(e1);
			}
			
		} catch (DataNodeClientException e) {
			
			try {
				// switch to another DN and try once more
				logger.error("Error: data node communication exception:"+e.getLocalizedMessage());
				logger.debug("Disable the DN and switch to another DN, if there is.");
				disableDN(dnAddress);
				dnAddress = getBetterDN(partitionClientAddrMap.get(parKey));
				if(dnAddress != null){
					DataNodeClient dn_client = clientAddrMap.get(dnAddress);
					long ts = System.currentTimeMillis();
					item = dn_client.queryItem(itemKey.getYear(), itemKey.getMonth(), itemKey.getSource()+itemKey.getSiteID(), itemKey.getForumID(),itemKey.getItemID());
					logDNResponseTimeAndCount(dnAddress, System.currentTimeMillis() - ts);
					return item;
				} else{
					logger.error("Fail to get data for Item Key: "+itemkey);
					throw new TEClientException("No active Data Node available.");
				}
			} catch (DataNodeClientCommunicationException e1) {
				logger.error("Fail to get data for Item Key: "+itemkey);
				logger.error("Error: data node communication exception:"+e.getLocalizedMessage());
				throw new TEClientException(e1);
			} catch (DataNodeClientException e1) {
				logger.error("Fail to get data for Item Key: "+itemkey);
				logger.error("Error: data node communication exception:"+e.getLocalizedMessage());
				throw new TEClientException(e1);
			}
			
		}
		
	}
	
	public TEItemEnumerator getItemEnumerator(String parKey, long startID,
			int count, boolean isdeleted) throws TEItemEnumeratorException {
		return new TEItemEnumerator(this.nndaemonAddr, this.nndaemonPort,
				parKey, startID, count, isdeleted);
	}
	
	public TEItemEnumerator getItemEnumerator(String parKey) throws TEItemEnumeratorException {
		return new TEItemEnumerator(this.nndaemonAddr, this.nndaemonPort,parKey);
	}
	
	public TEItemEnumerator getItemEnumerator(String parKey,long startID) throws TEItemEnumeratorException {
		return new TEItemEnumerator(this.nndaemonAddr,this.nndaemonPort,parKey,startID);
	}
	
	public TEItemEnumerator getItemEnumerator(String parKey,long startID,int count) throws TEItemEnumeratorException{
		return new TEItemEnumerator(this.nndaemonAddr,this.nndaemonPort,parKey,startID,count);
	}
	
	public void close()
	{
		try {
			nn_client.getDNAddressForQuery(0, 0, "", "");
		} catch (NameNodeClientException e) {
			// ignore
		}
		for(String key: clientAddrMap.keySet())
		{
			DataNodeClient dn_client = clientAddrMap.get(key);
			try {
				dn_client.queryItem(0, 0, "", "", 0);
			} catch (DataNodeClientCommunicationException e) {
				// ignore
			} catch (DataNodeClientException e) {
				// ignore
			}
		}
		clientAddrMap.clear();
	}

	
	private String getBetterDN(ArrayList<String> dnList){
		String dnAddress = null;
//		int readCount = -1;
		float resTime = -1;
		for(String tmp : dnList){
			if(this.disabledList.contains(tmp))	// 跳过无法读取的DN
				continue;
//			int count = this.clientReadCountMap.get(tmp);
//			if(readCount == -1){
//				readCount = count;
//				dnAddress = tmp;
//			} else{
//				if(count < readCount){
//					count = readCount;
//					dnAddress = tmp;
//				}
//			}
			float time = this.clientResponseTime.get(tmp);
			if(resTime == -1){
				resTime = time;
				dnAddress = tmp;
			} else{
				if(time < resTime){
					resTime = time;
					dnAddress = tmp;
				}
			}
		}
		return dnAddress;
	}
	
	private void disableDN(String dnAddress){
		this.disabledList.add(dnAddress);
	}
	
	private void logDNResponseTimeAndCount(String dnAddress, float res){
		int count = this.clientReadCountMap.get(dnAddress);
		float time = this.clientResponseTime.get(dnAddress);
		this.clientResponseTime.put(dnAddress, (time * count + res)/count);
		this.clientReadCountMap.put(dnAddress, count + 1);
	}
	
	public static void main(String[] args)
	{
		TEClient client = new TEClient("192.168.2.2", 6869);
		
		String url = "jdbc:mysql://192.168.1.12/LeoProject121";
		String user = "leo";
		String password = "cicdata";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, user, password);
			Statement stat = conn.createStatement();
			String sql = "select * from DS_ITEM where item_id > 600000000 limit 2000";
			stat.execute(sql);
			ResultSet result = stat.getResultSet();
			long current = System.currentTimeMillis();
			while(result.next())
			{
				String itemkey = result.getString("item_key");
				TEItem item = client.getItem(itemkey);
//				System.out.println(item.getMeta().getSiteID()+":"+item.getMeta().getForumID());
			}
			current = System.currentTimeMillis() - current;
			System.out.println(String.format("It takes %s ms to query 2000 items from TE.", current));
			
			conn.close();
			client.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TEClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
