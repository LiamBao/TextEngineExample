package com.cic.textengine.tefilter.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.cic.textengine.tefilter.TEItemFilter;
import com.cic.textengine.tefilter.exception.TEItemFilterException;
import com.cic.textengine.type.TEItem;

public class DefaultFilter implements TEItemFilter{

	private HashMap<String, ArrayList<String>> sitePosterMap = null;
	private HashMap<String, ArrayList<String>> sitePosteridMap = null;
	
	@Override
	public boolean accept(TEItem item) {
		
		long site = item.getMeta().getSiteID();
		String source = item.getMeta().getSource();
		String siteid = source+String.valueOf(site);
		
		String poster = item.getMeta().getPoster();
		String posterid = item.getMeta().getPosterID();
		
		if(sitePosterMap.keySet().contains(siteid)) {
			
			if(poster!=null && sitePosterMap.get(siteid).contains(poster))
				return true;
			
			if(posterid!=null && sitePosteridMap.get(siteid).contains(posterid))
				return true;
		}
		return false;
	}

	@Override
	public void close() throws TEItemFilterException {
		this.sitePosteridMap.clear();
		this.sitePosterMap.clear();
	}

	@Override
	public String getFilterName() {
		return "DefaultFilter";
	}

	@Override
	public void init() throws TEItemFilterException {
		// load the <te_site_id, poster_id> map
		
		sitePosterMap = new HashMap<String, ArrayList<String>>();
		sitePosteridMap = new HashMap<String, ArrayList<String>>();
		
		try {
			
			String url = "jdbc:mysql://192.168.1.52:3306/YY?useUnicode=true&characterEncoding=utf-8";
			String usr = "YY";
			String pwd = "cicdata";
			
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, usr, pwd);
			
			String sql = "select a.POSTER, a.POSTER_ID, b.TE_SOURCE, b.TE_SITE_ID from T_FIX_FORUM a, T_SOURCE b where a.SOURCE_ID = b.SOURCE_ID";
			Statement stat = conn.createStatement();
			stat.execute(sql);
			ResultSet result = stat.getResultSet();
			while(result.next()) {
				String source = result.getString("TE_SOURCE");
				long site = result.getLong("TE_SITE_ID");
				String siteid = source+String.valueOf(site);
				
				String poster = result.getString("POSTER");
				String posterid = result.getString("POSTER_ID");
				
				if(sitePosterMap.get(siteid) == null)
					sitePosterMap.put(siteid, new ArrayList<String>());
				if(sitePosteridMap.get(siteid) == null)
					sitePosteridMap.put(siteid, new ArrayList<String>());
				
				sitePosterMap.get(siteid).add(poster);
				sitePosteridMap.get(siteid).add(posterid);
			}
			
			stat.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			throw new TEItemFilterException(e);
		} catch (SQLException e) {
			throw new TEItemFilterException(e);
		}
		
	}
}
