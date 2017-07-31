package com.cic.textengine.datadelivery;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.type.TEItemMeta;


public class ProjectDB {

	private Connection conn = null;
	private Statement stat = null;

	private String url = null;
	private String user = null;
	private String password = null;

	private static Logger logger = Logger.getLogger(ProjectDB.class);
	private static String cfgFileName = "ProjectDB.properties";
	
	public ProjectDB(int projectID) throws ClassNotFoundException, SQLException, IOException {

		Class.forName("com.mysql.jdbc.Driver");
		String dbName = "LeoProject"+projectID;
		loadConfig();
		if(url.endsWith("/"))
			url = url.concat(dbName);
		else
			url = url.concat("/"+dbName);
		conn = DriverManager.getConnection(url, user, password);
		stat = conn.createStatement();
		logger.info("Connect to project database: "+dbName);
	}
	
	public void addMetaInfo(TEItemMeta meta) throws SQLException
	{
	  	long item_id = meta.getItemID();
		long site_id = meta.getSiteID();
		String forum_id = meta.getForumID();
		String poster = meta.getPoster();
		long dateofpost = meta.getDateOfPost();
		Date date = new Date(dateofpost);
		String item_type = meta.getItemType();
		String item_url = meta.getItemUrl();
		String source = meta.getSource();
		String keyterm = meta.getKeywordGroup();
		String keyword = meta.getKeyword();
		boolean istopicpost = meta.isTopicPost();
		String forumname = meta.getForumName();
		String forumurl = meta.getForumUrl();
		int year = meta.getYearOfPost();
		int month = meta.getMonthOfPost();
		
		ItemKey itemkey = new ItemKey(source, Long.toString(site_id), forum_id, year, month, item_id);
	
		String sql = String.format("INSERT into DS_ITEM (item_key, site_id, forum_id, poster, date_of_post, item_type, item_url, " +
				"source, keyterm, keyword, is_topic_post, forum_name, forum_url) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'" +
				", '%s', %s, '%s', '%s')", 
				itemkey.generateKey(), site_id, forum_id, poster, date, item_type, item_url, source, keyterm, keyword, istopicpost, forumname, forumurl);
		logger.info(sql);
		stat.execute(sql);
	}
	
	public void close() throws SQLException
	{
		stat.close();
		conn.close();
	}
	
	private void loadConfig() throws IOException
	{
		Properties properties = new Properties();
		InputStream is = null;
		is = ProjectDB.class.getResourceAsStream("/" +
				 cfgFileName);
		properties.load(is);
		is.close();
		
		url = properties.getProperty("url");
		user = properties.getProperty("user");
		password = properties.getProperty("password");
	}
}
