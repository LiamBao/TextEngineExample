package com.cic.textengine.itemreader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import com.cic.data.Item;
import com.cic.data.ItemMeta;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class DBItemReader implements ItemReader {
	private static final int FETCHSIZE = 20000;

	Connection dbConn = null;

	ResultSet resultSet = null;
	
	private Item currentItem = null;

	public DBItemReader(Connection conn, String sql) throws SQLException {
		dbConn = conn;
		Statement stat = dbConn.createStatement();
		stat.setFetchSize(FETCHSIZE);
		System.out.println(sql);
		String sqlCount = String.format("select count(0) from (%s) as z", sql
				.replaceAll("order by .+", ""));
		// String sqlCount = sql.replaceAll("\\*",
		// "count(0)").replaceAll("order by .+", "");
		resultSet = stat.executeQuery(sqlCount);
		resultSet.next();
		int totalSize = resultSet.getInt(1);
		System.out.println(String.format("%s items in total", totalSize));

		resultSet = stat.executeQuery(sql);
		System.out.println("Items loaded");		
	}

	public Item getItem() {
	
		return null; 
		
	}

	private static TEItem constructItem(ResultSet resultSet)
			throws SQLException {
		ItemMeta itemMeta = new TEItemMeta();
		TEItem item = new TEItem();
		long itemId = resultSet.getLong("itemid");
		int siteID = resultSet.getInt("siteid");
		String forumID = resultSet.getString("forumid");
		String subject = resultSet.getString("subject");
		String content = resultSet.getString("content");
		String poster = resultSet.getString("poster");
		Date dateOfPost = resultSet.getDate("dateofpost");
		String itemType = resultSet.getString("itemtype");
		String itemUrl = resultSet.getString("itemurl");
		boolean isTopicPost = resultSet.getBoolean("istopicpost");
		String siteName = "";
		String forumName = "";
		String keyword = "";
		String keywordGroup = "";
		poster = poster != null ? poster : "";

		try {
			siteName = resultSet.getString("sitename").toLowerCase();
			forumName = resultSet.getString("forumname").toLowerCase();
		} catch (Exception ex) {

		}

		// subject = subject !=
		// null?DscConvertUtil.full2Half(subject).toLowerCase():subject;
		// content = content !=
		// null?DscConvertUtil.full2Half(content).toLowerCase():content;

		itemMeta.setItemID(itemId);
		itemMeta.setSiteID(siteID);
		itemMeta.setForumID(forumID);
		itemMeta.setSubject(subject);
		itemMeta.setTopicPost(isTopicPost);
		itemMeta.setItemUrl(itemUrl);

		itemMeta.setDateOfPost(dateOfPost.getTime());

		itemMeta.setPoster(poster);
		itemMeta.setSiteName(siteName);
		itemMeta.setForumName(forumName);
		itemMeta.setKeyword(keyword);
		itemMeta.setKeywordGroup(keywordGroup);
		itemMeta.setItemType(itemType);

		item.setMeta(itemMeta);
		item.setSubject(subject);
		item.setContent(content);

		return item;
	}

	public boolean next() {
		return false;
	}

	public void close() throws Exception {
		if (dbConn != null)
			dbConn.close();
	}
}
