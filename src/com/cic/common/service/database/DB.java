package com.cic.common.service.database;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class DB {
	private static DB db;
	private Connection conn = null;	
	private static int JDBC_MYSQL = 1;
	private static String cfgFileName = "PostTrendDB.properties";
	private int JDBC = JDBC_MYSQL;
	private String url = null;
	private String user = null;
	private String password = null;

	private DB() {

	}

	private void initialize() throws ClassNotFoundException, SQLException, IOException {

//		System.out.println("To connect to post trend database.");
		loadConfig();
		if (JDBC == JDBC_MYSQL) {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
		}
	}

	public static void createConnection() throws Exception {
		if (db == null) {
			db = new DB();
			db.initialize();
		}
	}

	public static Connection getConnection(){		
		return db.conn;
	}


	private void loadConfig() throws IOException
	{
		Properties properties = new Properties();
		InputStream is = null;
		is = DB.class.getResourceAsStream("/" +
				 cfgFileName);
		properties.load(is);
		is.close();
		
		url = properties.getProperty("url");
		user = properties.getProperty("user");
		password = properties.getProperty("password");
	}
	
	public static void close() {
		if (db != null)
			try {
				db.conn.close();
			} catch (SQLException e) {
			} finally {
				db.conn = null;
				db = null;
			}			
	}
}