package com.cic.textengine.diagnose;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

import com.cic.common.service.database.DB;

public class PrintTotalVolume {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		DB.createConnection();
		Connection conn = DB.getConnection();
		Statement st = conn.createStatement();
		String sql = "SELECT SUM(POST_COUNT) FROM T_POSTTREND WHERE 1";
		st.execute(sql);
		ResultSet rs = st.getResultSet();
		if(rs.next()){
			System.out.println(String.format("Totally there are %s items in TE now.", rs.getLong(1)));
		}
		DB.close();
	}

}
