package com.cic.textengine.tefilter.queue.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.exception.TEClientException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.tefilter.queue.Queue;
import com.cic.textengine.tefilter.queue.exception.TEFilterQueueException;
import com.cic.textengine.type.TEItem;

public class DataBaseQueueImpl implements Queue{
	
	private String nnAddr = "192.168.2.2";
	private int port = 6869;
	private TEClient client = null;
	
	private String url = "jdbc:mysql://192.168.2.2/TEFilterResult";
	private String pwd = "Vj3tRws2";
	private String usr = "TENN002";
	private Connection conn = null;
	private Statement stat = null;
	private HashMap<String, ArrayList<String>> itemkeyMap = null;
	
	private static Logger logger = Logger.getLogger(DataBaseQueueImpl.class);

	@Override
	public void close() throws TEFilterQueueException {
		client.close();
		putItemKeyBack();
		try {
			stat.close();
			conn.close();
		} catch (SQLException e) {
		}
	}

	@Override
	public synchronized TEItem get(String filterName) throws TEFilterQueueException {
		String itemkey = null;
		TEItem item = null;
		
		try {
			if(itemkeyMap.get(filterName) == null  || itemkeyMap.get(filterName).size() <= 0) {
				 loadItemKey(filterName);
			}
		} catch (SQLException e) {
			// if fail, try to re-connect to the database
			conn = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(url, usr, pwd);
				stat = conn.createStatement();
				loadItemKey(filterName);
			} catch (ClassNotFoundException e1) {
				throw new TEFilterQueueException(e1);
			} catch (SQLException e2) {
				throw new TEFilterQueueException(e2);
			}
		}
			if(itemkeyMap.get(filterName).size() > 0) {
				itemkey = itemkeyMap.get(filterName).get(0);
				logger.debug(String.format("Reading Item: %s", itemkey));
				itemkeyMap.get(filterName).remove(0);
				try {
					item = client.getItem(itemkey);
				} catch (TEClientException e) {
					throw new TEFilterQueueException(e);
				}
			}
		
		return item;
	}

	@Override
	public void init() throws TEFilterQueueException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, usr, pwd);
			stat = conn.createStatement();
			
			client = new TEClient(nnAddr, port);
			
			itemkeyMap = new HashMap<String, ArrayList<String>>();
		} catch (ClassNotFoundException e) {
			throw new TEFilterQueueException(e);
		} catch (SQLException e) {
			throw new TEFilterQueueException(e);
		}
	}

	@Override
	public synchronized  void put(String filterName, String itemkey) throws TEFilterQueueException {
		String sql = String.format("insert into T_FILTER_RESULT (filter_name, item_key) values ('%s', '%s')", filterName, itemkey);
		try {
			stat.execute(sql);
		} catch (SQLException e) {
			throw new TEFilterQueueException(e);
		}
	}
	
	private void loadItemKey(String filterName) throws SQLException {

		ArrayList<String> itemkeyList = itemkeyMap.get(filterName);
		if(itemkeyList == null) {
			itemkeyList = new ArrayList<String>();
		}
		String sql = String.format("select * from T_FILTER_RESULT where filter_name = '%s' ORDER BY ID limit 1000", filterName);
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		int maxID = 0;
		while(result.next())
		{
			int id = result.getInt("ID");
			if (id > maxID) {
				maxID = id;
			}
			String itemkey = result.getString("Item_Key");
			itemkeyList.add(itemkey);
		}
		itemkeyMap.put(filterName, itemkeyList);
		sql = String.format("delete from T_FILTER_RESULT where filter_name = '%s' and ID < %s", filterName, maxID+1);
		stat.execute(sql);
		
	}
	
	private void putItemKeyBack() {
		for(String filterName: itemkeyMap.keySet()) {
			ArrayList<String> itemkeyList = itemkeyMap.get(filterName);
			if(itemkeyList.size() > 0) {
				for(String itemkey: itemkeyList) {
					try {
						put(filterName, itemkey);
					} catch (TEFilterQueueException e) {
						logger.error(String.format("Error insert the itemkey %s of filter %s", itemkey, filterName));
					}
				}
			}
		}
	}

}
