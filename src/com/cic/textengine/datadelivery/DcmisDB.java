package com.cic.textengine.datadelivery;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DcmisDB {
	
	private static DcmisDB db;
	private Connection conn = null;
	private static String cfgFileName = "DCMISdb.properties";
	private static Logger logger = Logger.getLogger(DcmisDB.class);
	
	private DcmisDB() {

	}
	
	private void initialize() throws ClassNotFoundException, SQLException, IOException {

		Properties properties = new Properties();
		InputStream is = null;
		is = DcmisDB.class.getResourceAsStream("/" +cfgFileName);
		properties.load(is);
		is.close();
		
		String url = properties.getProperty("url");
		String user = properties.getProperty("user");
		String password = properties.getProperty("password");
		
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection(url, user, password);

	}

	public static void createConnection() throws Exception {
		if (db == null) {
			db = new DcmisDB();
			db.initialize();
		}
	}

	public static Connection getConnection() throws Exception {
		if(db.conn == null){
			db = new DcmisDB();
			db.initialize();
		}
		return db.conn;
	}
	
	public static void close()
	{
		if (db != null)
			try {
				db.conn.close();
			} catch (Exception e) {
			} finally {
				db.conn = null;
				db = null;
				logger.debug("DCMIS DB connection closed.");
			}	
	}
}
