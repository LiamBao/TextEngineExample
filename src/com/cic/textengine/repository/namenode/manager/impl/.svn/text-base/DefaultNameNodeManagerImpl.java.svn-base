package com.cic.textengine.repository.namenode.manager.impl;

import java.beans.PropertyVetoException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.daemon.exception.NNDaemonException;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistry;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;
import com.cic.textengine.repository.namenode.manager.NameNodeManager;
import com.cic.textengine.repository.namenode.manager.exception.DataNodeIPAlreadyExistsException;
import com.cic.textengine.repository.namenode.manager.exception.IllegalNameNodeException;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.exception.NoDataNodeAvaliableForPartitionWrite;
import com.cic.textengine.repository.namenode.manager.type.DNPartitionUpgradeVersion;
import com.cic.textengine.repository.namenode.manager.type.OLogItem;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionClean;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionDelete;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionWrite;
import com.cic.textengine.repository.namenode.manager.type.Partition;
import com.cic.textengine.repository.type.PartitionKey;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DefaultNameNodeManagerImpl implements NameNodeManager {
	Logger m_logger = Logger.getLogger(DefaultNameNodeManagerImpl.class);
	static final String CONFIG_PROPERTIES_FILE = "NameNodeDaemon.properties";

	DataSource m_dataSource = null;

	String JDBC_URL = null;
	String JDBC_USER = null;
	String JDBC_PWD = null;
	int JDBC_POOL_SIZE = 10;
	int JDBC_POOL_MAX_IDLE_TIME = 300;
	
	int cacheSize = 10000;
	volatile int cacheHit = 0;
	volatile int totalQuery = 0;
	ConcurrentHashMap<String, Long> dnPartitionItemCountMap = null; // the cache for query DN partition item count.Default cache size is 10000.
	ConcurrentHashMap<String, Long> dnPartitionItemCountTTL = null; // the cache for query DN partition item count.Default cache size is 10000.
	
	public DefaultNameNodeManagerImpl() throws NameNodeManagerException {
		
		 dnPartitionItemCountMap = new ConcurrentHashMap<String, Long>();
		 dnPartitionItemCountTTL = new ConcurrentHashMap<String, Long>();
		
		 try {
			loadConfig();
		} catch (NNDaemonException e) {
			throw new NameNodeManagerException(e);
		}

		// configure data source connection pool
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
		} catch (PropertyVetoException e) {
			throw new NameNodeManagerException(e);
		}

		// loads the jdbc driver
		cpds.setJdbcUrl(JDBC_URL);
		cpds.setUser(JDBC_USER);
		cpds.setPassword(JDBC_PWD);
		cpds.setMaxPoolSize(JDBC_POOL_SIZE);
		cpds.setMaxIdleTime(JDBC_POOL_MAX_IDLE_TIME);
		cpds.setAutoCommitOnClose(false);
		cpds.setAutomaticTestTable("C3P0_TEST_TABLE");
		cpds.setIdleConnectionTestPeriod(120);

		m_dataSource = cpds;
	}

	/**
	 * Activate a data node by the dn key and dn ip.
	 * 
	 * @param dndaemon_ip
	 * @param key
	 * @throws NameNodeManagerException
	 * @throws IllegalNameNodeException
	 *             If the key and ip are not matched, this excpeiton will be
	 *             thrown
	 */
	public void activateNameNode(String dndaemon_ip, String key)
			throws NameNodeManagerException, IllegalNameNodeException {
		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn
					.prepareStatement(
							"SELECT * FROM T_DATANODE WHERE DAEMON_IP = ? AND DN_KEY = ?",
							ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_UPDATABLE);
			ps.setString(1, dndaemon_ip);
			ps.setString(2, key);
			ResultSet rs = ps.executeQuery();
			boolean found = false;
			if (rs.next()) {
				found = true;
				rs.updateTimestamp("LAST_ACTIVE_DT", new Timestamp(System
						.currentTimeMillis()));
				rs.updateRow();
			}

			rs.close();
			ps.close();
			conn.commit();
			conn.close();

			if (!found) {
				throw new IllegalNameNodeException(dndaemon_ip, key);
			}
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}

	/**
	 * Assign a partition to a data node
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param dnkey
	 * @throws NameNodeManagerException
	 */
	public void assignDNPartition(int year, int month, String siteid,
			String forumid, String dnkey) throws NameNodeManagerException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			Partition partition = this.getPartition(conn, year, month, siteid,
					forumid);
			if (partition == null) {
				throw new NameNodeManagerException("Can't find partition.");
			}

			ps = conn.prepareStatement("SELECT * FROM T_DN_PARTITION WHERE "
					+ " PARTITION_ID = ? AND DN_KEY = ?",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			ps.setInt(1, partition.getID());
			ps.setString(2, dnkey);
			rs = ps.executeQuery();

			if (!rs.next()) {
				rs.moveToInsertRow();
				rs.updateString("DN_KEY", dnkey);
				rs.updateInt("PARTITION_ID", partition.getID());
				rs.updateInt("PARTITION_ITEM_COUNT", 0);
				rs.updateInt("VERSION", 0);
				rs.insertRow();
			}

			rs.close();
			rs = null;
			ps.close();
			ps = null;
			conn.commit();
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}

	public synchronized int cleanPartition(int year, int month, String siteid,
			String forumid) throws NameNodeManagerException {
		Partition partition = getPartition(year, month, siteid, forumid);
		if (partition == null) {// can not find the partition.
			throw new NameNodeManagerException("Can't find partition.");
		}

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			OLogPartitionClean olog = new OLogPartitionClean();
			olog.setPartitionID(partition.getID());
			olog.setVersion(partition.getVersion() + 1);

			logOperation(conn, olog, year, month, siteid, forumid);

			ps = conn
					.prepareStatement("UPDATE T_DN_PARTITION SET "
							+ " PARTITION_ITEM_COUNT = ?, VERSION = ?, TS_LAST_MODIFY = ? "
							+ " WHERE PARTITION_ID = ?");
			ps.setLong(1, 0);
			ps.setInt(2, olog.getVersion());
			ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			ps.setInt(4, olog.getPartitionID());
			ps.executeUpdate();
			ps.close();
			ps = null;

			ps = conn
					.prepareStatement("UPDATE T_PARTITION SET VERSION = ?, TS_LAST_MODIFY = ?, ITEM_COUNT = ? "
							+ " WHERE PARTITION_ID = ?");
			ps.setInt(1, olog.getVersion());
			ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			ps.setLong(3, 0);
			ps.setInt(4, olog.getPartitionID());
			ps.executeUpdate();
			ps.close();
			ps = null;
			
			// remove the item count cache if necessary
			ps = conn.prepareStatement("select DN_KEY from T_DN_PARTITION where PARTITION_ID = ?");
			ps.setInt(1, olog.getPartitionID());
			ResultSet rs = ps.executeQuery();

			String dn_parkey = null;
			String parkey = (new PartitionKey(year, month, siteid, forumid)).generateStringKey();
			while(rs.next()) {
				String dn_key = rs.getString("DN_KEY");
				dn_parkey = dn_key + parkey;
//				this.updateDNPartitionItemCount(dn_parkey, 0);
				this.removeDNPartitoinItemCount(dn_parkey);
			}
			
			ps.close();
			rs.close();
			ps = null;
			
			conn.commit();

			return olog.getVersion();
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} catch (IOException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}

	Partition createPartition(int year, int month, String siteid, String forumid)
			throws NameNodeManagerException {
		Connection conn = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			PreparedStatement ps = conn.prepareStatement(
					"SELECT * FROM T_PARTITION WHERE " + " THE_YEAR = ? AND "
							+ " THE_MONTH = ? AND " 
							+ " BINARY SITE_ID = ?  AND "
							+ " BINARY FORUM_ID = ?", 
							ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_UPDATABLE);

			ps.setInt(1, year);
			ps.setInt(2, month);
			ps.setString(3, siteid);
			ps.setString(4, forumid);

			ResultSet rs = ps.executeQuery();
			Partition partition = null;
			if (rs.next()) {
				partition = new Partition();
				partition.setForumID(forumid);
				partition.setID(rs.getInt("PARTITION_ID"));
				partition.setMonth(month);
				partition.setSiteID(siteid);
				partition.setYear(year);
				partition.setVersion(rs.getInt("VERSION"));
				partition.setItemCount(rs.getLong("ITEM_COUNT"));
			} else {
				rs.moveToInsertRow();
				rs.updateInt("THE_YEAR", year);
				rs.updateInt("THE_MONTH", month);
				rs.updateString("SITE_ID", siteid);
				rs.updateString("FORUM_ID", forumid);
				rs.updateTimestamp("CREATE_DT", new Timestamp(System
						.currentTimeMillis()));
				rs.updateLong("ITEM_COUNT", 0);
				rs.insertRow();
				partition = new Partition();
				partition.setID(rs.getInt("PARTITION_ID"));
				partition.setForumID(forumid);
				partition.setMonth(month);
				partition.setSiteID(siteid);
				partition.setYear(year);
				partition.setVersion(0);
				partition.setItemCount(0);
			}
			rs.close();
			ps.close();
			conn.commit();
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {// ignore
			}
		}
		return this.getPartition(year, month, siteid, forumid);
	}

	public synchronized int finishPartitionDelete(int year, int month,
			String siteid, String forumid, int targetVersion,
			ArrayList<String> dnkeys) throws NameNodeManagerException {
		Partition partition = getPartition(year, month, siteid, forumid);
		if (partition == null) {// can not find the partition.
			throw new NameNodeManagerException("Can't find partition.");
		}
		if (partition.getVersion() != targetVersion) {
			throw new NameNodeManagerException(
					"Illegal status of partition. Need to check.");
		}

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			ps = conn
					.prepareStatement("SELECT * FROM T_PARTITION_OPERATION_LOG WHERE PARTITION_ID = ? AND VERSION = ?");
			ps.setInt(1, partition.getID());
			ps.setInt(2, targetVersion);
			rs = ps.executeQuery();
			if (!rs.next()) {
				throw new NameNodeManagerException(
						"No corresponding partition delete operation log found.");
			}
			if (rs.getInt("OPERATION_TYPE") != 3) {
				throw new NameNodeManagerException(
						"No corresponding partition delete operation log found.");
			}
			rs.close();
			rs = null;
			ps.close();
			ps = null;

			ps = conn.prepareStatement("UPDATE T_DN_PARTITION SET "
					+ " VERSION = ?, TS_LAST_MODIFY = ? "
					+ " WHERE PARTITION_ID = ? AND DN_KEY = ?");

			ps.setInt(1, targetVersion);
			ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			ps.setInt(3, partition.getID());

			for (int i = 0; i < dnkeys.size(); i++) {
				ps.setString(4, dnkeys.get(i));
				ps.executeUpdate();
			}
			ps.close();
			ps = null;

			conn.commit();

			return targetVersion;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}

	/**
	 * Finish the partition write operation
	 */
	public synchronized int finishPartitionWrite(int year, int month,
			String siteid, String forumid, long startItemID, long itemCount,
			ArrayList<String> DNKeyList) throws NameNodeManagerException {
		Partition partition = this.getPartition(year, month, siteid, forumid);
		if (partition == null) {
			throw new NameNodeManagerException("Can't find partition.");
		}

		if (partition.getItemCount() != startItemID - 1) {
			throw new NameNodeManagerException(
					"Illegal partition status. Partition currently has "
							+ partition.getItemCount());
		}

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			for (int i = 0; i < DNKeyList.size(); i++) {
				if (!isDNPartitionVersionLatest(conn, partition.getID(),
						DNKeyList.get(i))) {
					throw new NameNodeManagerException(
							"Illegal status of data node for partition write:["
									+ DNKeyList.get(i));
				}
			}

			OLogPartitionWrite olog = new OLogPartitionWrite();
			olog.setItemCount(itemCount);
			olog.setStartItemID(startItemID);
			olog.addSeedDNKeys(DNKeyList);
			olog.setPartitionID(partition.getID());
			olog.setVersion(partition.getVersion() + 1);

			logOperation(conn, olog, year, month, siteid, forumid);

			ps = conn
					.prepareStatement("UPDATE T_DN_PARTITION SET "
							+ " PARTITION_ITEM_COUNT = ?, VERSION = ?, TS_LAST_MODIFY = ? "
							+ " WHERE DN_KEY = ? AND PARTITION_ID = ?");

			String dn_parkey = null;
			String parkey = (new PartitionKey(year, month, siteid, forumid)).generateStringKey();
			for (int i = 0; i < DNKeyList.size(); i++) {
				ps.setLong(1, olog.getItemCount() + olog.getStartItemID() - 1);
				ps.setInt(2, olog.getVersion());
				ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
				ps.setString(4, DNKeyList.get(i));
				ps.setInt(5, olog.getPartitionID());
				ps.executeUpdate();
				dn_parkey = DNKeyList.get(i) + parkey;
				this.updateDNPartitionItemCount(dn_parkey,  olog.getItemCount() + olog.getStartItemID() - 1);
			}
			ps.close();
			ps = null;

			ps = conn.prepareStatement("UPDATE T_PARTITION SET VERSION = ?, "
					+ " TS_LAST_MODIFY = ?, ITEM_COUNT = ? "
					+ " WHERE PARTITION_ID = ?");

			ps.setInt(1, olog.getVersion());
			ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			ps.setLong(3, olog.getItemCount() + olog.getStartItemID() - 1);
			ps.setInt(4, olog.getPartitionID());
			ps.executeUpdate();
			ps.close();
			ps = null;

			conn.commit();

			return olog.getVersion();

		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} catch (IOException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				// ignore
			}
		}
	}

	/**
	 * Retrieve a data node for write items into the partition.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeManagerException
	 */
	public ArrayList<String> getDataNodeForPartitionWrite(int year, int month,
			String siteid, String forumid) throws NameNodeManagerException,
			NoDataNodeAvaliableForPartitionWrite {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			Partition partition = getPartition(conn, year, month, siteid,
					forumid);

			if (partition == null) {
				m_logger.debug("Can not find a partition for write, create an empty partition [Y:" 
						+ year + ",M:" + month + ",S:" + siteid + ",F" + forumid + "]");
				partition = this.createPartition(year, month, siteid, forumid);
			}

			ps = conn
					.prepareStatement("SELECT b.DN_KEY FROM T_DN_PARTITION a, T_DATANODE b"
							+ " WHERE a.DN_KEY = b.DN_KEY AND "
							+ " PARTITION_ID = ? AND VERSION = ?");

			ps.setInt(1, partition.getID());
			ps.setInt(2, partition.getVersion());

			rs = ps.executeQuery();

			ArrayList<String> res = new ArrayList<String>();
			while (rs.next()) {
				res.add(rs.getString("DN_KEY"));
			}
			rs.close();
			rs = null;
			ps.close();
			ps = null;

			if (res.size() <= 0 && partition.getVersion() > 0) {
				throw new NoDataNodeAvaliableForPartitionWrite(
						"All data nodes don't have the latest partition version.");
			}

			conn.commit();
			return res;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {// ignore
			}
		}
	}

	/**
	 * Check if a data node is available for appending new items.
	 * 
	 * Some time one data node will be contains the most updated items. In this
	 * case this data node can not be used to append new data.
	 * 
	 * @param dnkey
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return Return -1 if this data node doesnot contains the latest partition
	 *         data or it is not registered for this partition. Otherwise return
	 *         the partition append point (startItemID).
	 * @throws NameNodeManagerException
	 */
	public long getDNPartitionAppendPoint(String dnkey, int year, int month,
			String siteid, String forumid) throws NameNodeManagerException {
		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn
					.prepareStatement("SELECT a.DN_KEY, a.PARTITION_ITEM_COUNT FROM T_DN_PARTITION a, T_PARTITION b  "
							+ " WHERE a.PARTITION_ID = b.PARTITION_ID  "
							+ " AND b.THE_YEAR = ?"
							+ " AND b.THE_MONTH = ?"
							+ " AND BINARY b.SITE_ID = ? " 
							+ " AND BINARY b.FORUM_ID = ? ");
			ps.setInt(1, year);
			ps.setInt(2, month);
			ps.setString(3, siteid);
			ps.setString(4, forumid);

			ResultSet rs = ps.executeQuery();

			long max_item_count = 0;
			long dn_item_count = -1;

			long item_count = 0;

			while (rs.next()) {
				item_count = rs.getLong("PARTITION_ITEM_COUNT");
				if (rs.getString("DN_KEY").equals(dnkey)) {
					dn_item_count = item_count;
				}
				if (item_count > max_item_count) {
					max_item_count = item_count;
				}
			}

			rs.close();
			ps.close();
			conn.close();

			if (dn_item_count != max_item_count || dn_item_count < 0) {
				return -1;
			} else {
				return dn_item_count + 1;
			}
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}

	public long getDNPartitionItemCount(String dnkey, int year, int month,
			String siteid, String forumid) throws NameNodeManagerException {
		
		this.totalQuery += 1;
		
		String parkey = (new PartitionKey(year, month, siteid, forumid)).generateStringKey();
		String dn_parkey = dnkey + parkey;
		
		if(this.dnPartitionItemCountMap.keySet().contains(dn_parkey)) {
			// check if the dn_parkey is cached.
			this.cacheHit += 1;
			this.dnPartitionItemCountTTL.replace(dn_parkey, System.currentTimeMillis()); // update the TTL
			return this.dnPartitionItemCountMap.get(dn_parkey);
		}

		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn
					.prepareStatement("SELECT A.PARTITION_ITEM_COUNT FROM T_DN_PARTITION A, T_PARTITION B WHERE "
							+ " A.PARTITION_ID = B.PARTITION_ID AND "
							+ " A.DN_KEY = ? AND "
							+ " B.THE_YEAR = ? AND "
							+ " B.THE_MONTH = ? AND "
							+ " BINARY B.SITE_ID = ?  AND "
							+ " BINARY B.FORUM_ID = ?  ");
			ps.setString(1, dnkey);
			ps.setInt(2, year);
			ps.setInt(3, month);
			ps.setString(4, siteid);
			ps.setString(5, forumid);

			ResultSet rs = ps.executeQuery();
			long result = 0;
			if (rs.next()) {
				result = rs.getLong("PARTITION_ITEM_COUNT");
			}

			rs.close();
			ps.close();
			conn.commit();
			conn.close();
			
			cacheDNPartitionItemCount(dn_parkey, result);

			return result;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}

	public synchronized OLogItem getNextDNPartitionOperation(String dnkey,
			int year, int month, String siteid, String forumid)
			throws NameNodeManagerException {
		Partition partition = getPartition(year, month, siteid, forumid);
		if (partition == null) {// can not find the partition.
			throw new NameNodeManagerException("Can't find partition.");
		}

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		int version = 0;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			ps = conn
					.prepareStatement("SELECT VERSION FROM T_DN_PARTITION WHERE "
							+ " PARTITION_ID = ? AND DN_KEY = ?");
			ps.setInt(1, partition.getID());
			ps.setString(2, dnkey);

			rs = ps.executeQuery();

			int dn_partition_version = 0;
			if (rs.next()) {
				dn_partition_version = rs.getInt("VERSION");
				if (partition.getVersion() > dn_partition_version)
					version = dn_partition_version + 1;
			}

			if (version == 0) {
				return null;
			}

			rs.close();
			rs = null;
			ps.close();
			ps = null;

			ps = conn
					.prepareStatement("SELECT VERSION FROM  T_PARTITION_OPERATION_LOG WHERE PARTITION_ID = ? AND VERSION > ? AND OPERATION_TYPE = 2");
			ps.setInt(1, partition.getID());
			ps.setInt(2, version);
			
			rs = ps.executeQuery();
			int clean_version = -1;
			while(rs.next()) {
				int tmp_clean_version = rs.getInt("VERSION");
				if(tmp_clean_version > clean_version)
					clean_version = tmp_clean_version;
//				version = clean_version;
			}
			
			if(clean_version > 0) {
				version = clean_version;
				Statement stat = conn.createStatement();
//				if(dn_partition_version == 0) {
//					stat
//					.execute(String
//							.format(
//									"insert into T_DN_PARTITION (DN_KEY, PARTITION_ID, PARTITION_ITEM_COUNT, VERSION) values ('%s', %s, %s, %s))", dnkey, partition.getID(), 0, version));
//				} else {
					stat
							.execute(String
									.format(
											"UPDATE T_DN_PARTITION SET VERSION = %s, PARTITION_ITEM_COUNT = 0 WHERE PARTITION_ID = %s AND DN_KEY = '%s'",
											version, partition.getID(), dnkey));
//				}
				stat.close();
				version ++;
			}
			
			rs.close();
			rs = null;
			ps.close();
			ps = null;
			
			ps = conn
					.prepareStatement("SELECT * FROM T_PARTITION_OPERATION_LOG "
							+ "WHERE PARTITION_ID = ? AND VERSION = ?");
			ps.setInt(1, partition.getID());
			ps.setInt(2, version);

			rs = ps.executeQuery();
			int operation;
			if (rs.next()) {
				operation = rs.getInt("OPERATION_TYPE");
				switch (operation) {
				case 1:
					OLogPartitionWrite olog_write = new OLogPartitionWrite();
					olog_write.setPartitionID(partition.getID());
					olog_write.setVersion(version);
					olog_write.readFields(rs.getBinaryStream("OPERATION_DATA"));

					olog_write.cleanSeedDNKeys();
					olog_write.addSeedDNKeys(listDNKeysWithPartitionVersion(
							conn, partition.getID(), version));
					return olog_write;
//				case 2:
//					Statement stat = conn.createStatement();
//					stat
//							.execute(String
//									.format(
//											"UPDATE T_DN_PARTITION SET VERSION = %s, PARTITION_ITEM_COUNT = 0 WHERE PARTITION_ID = %s AND DN_KEY = %s",
//											version, partition.getID(), dnkey));
//					OLogPartitionClean olog_clean = new OLogPartitionClean();
//					olog_clean.setPartitionID(partition.getID());
//					olog_clean.setVersion(version);
//					olog_clean.readFields(rs.getBinaryStream("OPERATION_DATA"));
//					return olog_clean;
				case 3:
					OLogPartitionDelete olog_delete = new OLogPartitionDelete();
					olog_delete.setPartitionID(partition.getID());
					olog_delete.setVersion(version);
					olog_delete
							.readFields(rs.getBinaryStream("OPERATION_DATA"));
					return olog_delete;
				}
			}

			conn.commit();

			return null;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} catch (IOException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}

	Partition getPartition(Connection conn, int year, int month, String siteid,
			String forumid) throws SQLException {
		PreparedStatement ps = conn
				.prepareStatement("SELECT * FROM T_PARTITION WHERE "
						+ " THE_YEAR = ? AND " + " THE_MONTH = ? AND "
						+ " BINARY SITE_ID = ?  AND " 
						+ " BINARY FORUM_ID = ?   ");

		ps.setInt(1, year);
		ps.setInt(2, month);
		ps.setString(3, siteid);
		ps.setString(4, forumid);

		ResultSet rs = ps.executeQuery();
		Partition partition = null;
		if (rs.next()) {
			partition = new Partition();
			partition.setForumID(forumid);
			partition.setID(rs.getInt("PARTITION_ID"));
			partition.setMonth(month);
			partition.setSiteID(siteid);
			partition.setYear(year);
			partition.setVersion(rs.getInt("VERSION"));
			partition.setItemCount(rs.getLong("ITEM_COUNT"));
		}
		rs.close();
		ps.close();
		return partition;
	}

	public Partition getPartition(int year, int month, String siteid,
			String forumid) throws NameNodeManagerException {
		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn
					.prepareStatement("SELECT * FROM T_PARTITION WHERE "
							+ " THE_YEAR = ? AND " + " THE_MONTH = ? AND "
							+ " BINARY SITE_ID = ?  AND " 
							+ " BINARY FORUM_ID = ?  ");

			ps.setInt(1, year);
			ps.setInt(2, month);
			ps.setString(3, siteid);
			ps.setString(4, forumid);

			ResultSet rs = ps.executeQuery();
			Partition partition = null;
			if (rs.next()) {
				partition = new Partition();
				partition.setForumID(forumid);
				partition.setID(rs.getInt("PARTITION_ID"));
				partition.setMonth(month);
				partition.setSiteID(siteid);
				partition.setYear(year);
				partition.setVersion(rs.getInt("VERSION"));
				partition.setItemCount(rs.getLong("ITEM_COUNT"));
			}
			rs.close();
			ps.close();
			conn.close();

			return partition;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}

	/**
	 * Check if the dn has the most last partition version
	 * 
	 * @param conn
	 * @param partition_id
	 * @return
	 */
	boolean isDNPartitionVersionLatest(Connection conn, int partition_id,
			String dnkey) throws SQLException {
		PreparedStatement ps = conn
				.prepareStatement("SELECT a.VERSION as DN_VERSION, b.VERSION as PT_VERSION FROM T_DN_PARTITION a, T_PARTITION b "
						+ " WHERE a.PARTITION_ID = b.PARTITION_ID AND b.PARTITION_ID = ? AND a.DN_KEY = ?");
		ps.setInt(1, partition_id);
		ps.setString(2, dnkey);
		ResultSet rs = ps.executeQuery();
		boolean result = false;
		if (rs.next()) {
			if (rs.getInt("DN_VERSION") == rs.getInt("PT_VERSION")) {
				result = true;
			}
		}
		rs.close();
		ps.close();
		return result;
	}

	public boolean isDNPartitionVersionLatest(int year, int month,
			String siteid, String forumid, String dnkey)
			throws NameNodeManagerException {
		Partition partition = this.getPartition(year, month, siteid, forumid);
		if (partition == null) {
			throw new NameNodeManagerException("Can't find partition.");
		}

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			return isDNPartitionVersionLatest(conn, partition.getID(), dnkey);

		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				// ignore
			}
		}
	}

	ArrayList<String> listDNKeysWithPartitionVersion(Connection conn,
			int partition_id, int version) throws SQLException {
		PreparedStatement ps = conn
				.prepareStatement("SELECT DN_KEY FROM T_DN_PARTITION WHERE PARTITION_ID = ? AND VERSION >= ?");
		ps.setInt(1, partition_id);
		ps.setInt(2, version);
		ResultSet rs = ps.executeQuery();
		ArrayList<String> res = new ArrayList<String>();
		while (rs.next()) {
			res.add(rs.getString("DN_KEY"));
		}
		rs.close();
		ps.close();
		return res;
	}

	public ArrayList<DNPartitionUpgradeVersion> listDNPartitionUpgradeVersion(
			String dnkey) throws NameNodeManagerException {
		ArrayList<DNPartitionUpgradeVersion> result = new ArrayList<DNPartitionUpgradeVersion>();
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			ps = conn
					.prepareStatement("SELECT B.THE_YEAR, B.THE_MONTH, B.SITE_ID, B.FORUM_ID, " +
							" B.VERSION AS PAT_VERSION, A.VERSION AS DN_VERSION "
							+ " FROM T_DN_PARTITION A, T_PARTITION B WHERE "
							+ " A.PARTITION_ID = B.PARTITION_ID "
							+ " AND A.VERSION < B.VERSION "
							+ "AND A.DN_KEY = ?");
			ps.setString(1, dnkey);

			rs = ps.executeQuery();
			int year, month,pat_version;
			String siteid, forumid;
			
			
			while (rs.next()) {
				year = rs.getInt("THE_YEAR");
				month = rs.getInt("THE_MONTH");
				siteid = rs.getString("SITE_ID");
				forumid = rs.getString("FORUM_ID");
				pat_version = rs.getInt("PAT_VERSION");

				DNPartitionUpgradeVersion dnpv = new DNPartitionUpgradeVersion();
				dnpv.setDNKey(dnkey);
				dnpv.setYear(year);
				dnpv.setMonth(month);
				dnpv.setSiteid(siteid);
				dnpv.setForumid(forumid);
				dnpv.setVersion(pat_version);
				result.add(dnpv);
			}

			conn.commit();

			return result;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}

	/**
	 * List all data nodes which contain the partition for query partition data
	 */
	public ArrayList<String> listPartitionDNForQuery(int year, int month,
			String siteid, String forumid) throws NameNodeManagerException {
		ArrayList<String> result = new ArrayList<String>();

		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn
					.prepareStatement("SELECT A.DN_KEY FROM T_DN_PARTITION A, T_PARTITION B WHERE "
							+ " A.PARTITION_ID = B.PARTITION_ID AND "
							+ " B.THE_YEAR = ? AND "
							+ " B.THE_MONTH = ? AND "
							+ " BINARY B.SITE_ID = ?  AND "
							+ " BINARY B.FORUM_ID = ?  AND "
							+ " A.VERSION = B.VERSION ");
			ps.setInt(1, year);
			ps.setInt(2, month);
			ps.setString(3, siteid);
			ps.setString(4, forumid);

			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				result.add(rs.getString("DN_KEY"));
			}

			rs.close();
			ps.close();
			conn.commit();
			conn.close();

			return result;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}
	
	
	/**
	 * List all data nodes which contain the partition.
	 */
	public ArrayList<String> listPartitionDN(int year, int month,
			String siteid, String forumid) throws NameNodeManagerException {
		ArrayList<String> result = new ArrayList<String>();

		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn
					.prepareStatement("SELECT A.DN_KEY FROM T_DN_PARTITION A, T_PARTITION B WHERE "
							+ " A.PARTITION_ID = B.PARTITION_ID AND "
							+ " B.THE_YEAR = ? AND "
							+ " B.THE_MONTH = ? AND "
							+ " BINARY B.SITE_ID = ?  AND "
							+ " BINARY B.FORUM_ID = ?  " +
							"ORDER BY A.VERSION DESC");
			ps.setInt(1, year);
			ps.setInt(2, month);
			ps.setString(3, siteid);
			ps.setString(4, forumid);

			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				result.add(rs.getString("DN_KEY"));
			}

			rs.close();
			ps.close();
			conn.commit();
			conn.close();

			return result;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}

	void loadConfig() throws NNDaemonException {
		Properties props = new Properties();

		InputStream is = this.getClass().getResourceAsStream(
				"/" + CONFIG_PROPERTIES_FILE);
		try {
			props.load(is);
			is.close();
		} catch (IOException e) {
			throw new NNDaemonException(e);
		}

		this.JDBC_URL = props.getProperty("Repository.JDBC.URL");
		this.JDBC_USER = props.getProperty("Repository.JDBC.USER");
		this.JDBC_PWD = props.getProperty("Repository.JDBC.PWD");
		this.JDBC_POOL_SIZE = Integer.parseInt(props
				.getProperty("Repository.JDBC.POOL.SIZE"));
	}

	/**
	 * Operational log on the partition.
	 * 
	 * @param conn
	 * @param logitem
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @throws SQLException
	 * @throws IOException
	 */
	void logOperation(Connection conn, OLogItem logitem, int year, int month,
			String siteid, String forumid) throws SQLException, IOException {

		PreparedStatement ps = null;
		ResultSet rs = null;

		ps = conn.prepareStatement("SELECT * FROM T_PARTITION_OPERATION_LOG  "
				+ "WHERE PARTITION_ID = ? AND VERSION = ?",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

		ps.setInt(1, logitem.getPartitionID());
		ps.setInt(2, logitem.getVersion());

		rs = ps.executeQuery();
		boolean insert = false;
		if (!rs.next()) {
			insert = true;
			rs.moveToInsertRow();
		}

		rs.updateInt("PARTITION_ID", logitem.getPartitionID());
		rs.updateInt("VERSION", logitem.getVersion());
		rs.updateInt("OPERATION_TYPE", logitem.getType());
		rs.updateTimestamp("TS", new Timestamp(System.currentTimeMillis()));
		byte[] buff;
		buff = logitem.getData();

		rs.updateBinaryStream("OPERATION_DATA", new ByteArrayInputStream(buff),
				buff.length);
		if (insert) {
			rs.insertRow();
		} else {
			rs.updateRow();
		}
	}

	public synchronized int logPartitionDeleteOperation(int year, int month,
			String siteid, String forumid, ArrayList<Long> itemIDList,
			boolean IDSorted) throws NameNodeManagerException {
		Partition partition = getPartition(year, month, siteid, forumid);
		if (partition == null) {// can not find the partition.
			throw new NameNodeManagerException("Can't find partition.");
		}

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			OLogPartitionDelete olog = new OLogPartitionDelete();
			olog.setPartitionID(partition.getID());
			olog.setVersion(partition.getVersion() + 1);
			olog.addItemIDList(itemIDList);
			olog.setSorted(IDSorted);
			logOperation(conn, olog, year, month, siteid, forumid);

			ps = conn
					.prepareStatement("UPDATE T_PARTITION SET VERSION = ?, TS_LAST_MODIFY = ? "
							+ " WHERE PARTITION_ID = ?");
			ps.setInt(1, olog.getVersion());
			ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			ps.setInt(3, partition.getID());
			ps.executeUpdate();
			ps.close();
			ps = null;

			conn.commit();

			return olog.getVersion();
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} catch (IOException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (conn != null)
					conn.close();
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
			}
		}
	}

	/**
	 * Register a new DataNode daemon. If the ip address already exists,
	 * exception will be thrown.
	 * 
	 * @throws DataNodeIPAlreadyExistsException
	 */
	public String regsiterDataNode(String dndaemon_ip, int dndaemon_port)
			throws NameNodeManagerException, DataNodeIPAlreadyExistsException {
		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn.prepareStatement(
					"SELECT * FROM T_DATANODE WHERE DAEMON_IP = ?",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			ps.setString(1, dndaemon_ip);
			ResultSet rs = ps.executeQuery();
			boolean ip_duplicated = false;
			if (rs.next()) {
				ip_duplicated = true;
			}

			// generate a new key
			String key = UUID.randomUUID().toString();

			if (!ip_duplicated) {
				rs.moveToInsertRow();
				rs.updateString("DN_KEY", key);
				rs.updateTimestamp("REGISTER_DT", new Timestamp(System
						.currentTimeMillis()));
				rs.updateString("DAEMON_IP", dndaemon_ip);
				rs.updateInt("DAEMON_PORT", dndaemon_port);
				rs.insertRow();
			}
			rs.close();
			ps.close();
			conn.commit();
			conn.close();

			if (ip_duplicated) {
				throw new DataNodeIPAlreadyExistsException();
			}

			return key;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}

	/**
	 * Upgrade the partition version for a partition to one version above.
	 * 
	 * @param dnkey
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param version
	 * @throws NameNodeManagerException
	 */
	public synchronized void updateDNPartitionVersion(String dnkey, int year,
			int month, String siteid, String forumid, long itemCount,
			int version) throws NameNodeManagerException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);

			Partition partition = this.getPartition(conn, year, month, siteid,
					forumid);
			if (partition == null) {
				throw new NameNodeManagerException("Can't find partition.");
			}

			ps = conn.prepareStatement("SELECT * FROM T_DN_PARTITION WHERE "
					+ " PARTITION_ID = ? AND DN_KEY = ?",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			ps.setInt(1, partition.getID());
			ps.setString(2, dnkey);
			rs = ps.executeQuery();
			int dn_par_version = 0;
			if (rs.next()) {
				dn_par_version = rs.getInt("VERSION");
				if (rs.getLong("PARTITION_ITEM_COUNT") > itemCount
						&& itemCount != 0) {
					throw new NameNodeManagerException(
							"Can not update partition to a version with smaller item count.");
				}
//				allow the version updated uncontinously to avoid the clean operation issue.
//				if (dn_par_version != (version - 1)) {
//					throw new NameNodeManagerException(
//							"Can't not update DN partition to a "
//									+ "uncontinoused version. [DN_PAR_VER:"
//									+ dn_par_version + ", TO_VERSION:"
//									+ version);
//				} else {
					rs.updateInt("VERSION", version);
					if (itemCount > 0) {
						rs.updateLong("PARTITION_ITEM_COUNT", itemCount);
						// update the cache if it is there
						String parkey = (new PartitionKey(year, month, siteid, forumid)).generateStringKey();
						String dn_parkey = dnkey+parkey;
						this.updateDNPartitionItemCount(dn_parkey, itemCount);
					}
					rs.updateTimestamp("TS_LAST_MODIFY", new Timestamp(System
							.currentTimeMillis()));
					rs.updateRow();
//				}
			} else {
				throw new NameNodeManagerException(
						"DN is not registered with this partition.");
			}

			rs.close();
			rs = null;
			ps.close();
			ps = null;

			conn.commit();
			
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}

	public boolean validateDataNode(String key, String ip)
			throws NameNodeManagerException {
		Connection conn;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps = conn
					.prepareStatement("SELECT * FROM T_DATANODE WHERE DAEMON_IP = ? AND DN_KEY = ?");
			ps.setString(1, ip);
			ps.setString(2, key);
			ResultSet rs = ps.executeQuery();
			boolean found = false;
			if (rs.next()) {
				found = true;
			}

			rs.close();
			ps.close();
			conn.commit();
			conn.close();

			return found;
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		}
	}

	public int getDNPartitionCount(String dnkey)
			throws NameNodeManagerException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		int result = 0;
		try {
			conn = m_dataSource.getConnection();
			conn.setAutoCommit(false);


			ps = conn.prepareStatement("SELECT COUNT(0) AS NUM FROM T_DN_PARTITION WHERE "
					+ " DN_KEY = ?");
			ps.setString(1, dnkey);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUM");
			} 

			rs.close();
			rs = null;
			ps.close();
			ps = null;

			conn.commit();
		} catch (SQLException e) {
			throw new NameNodeManagerException(e);
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
		return result;
	}
	
	private synchronized void cacheDNPartitionItemCount(String dn_parkey, long count) {
		long current = System.currentTimeMillis();
		m_logger.debug(String.format("New dn_parkey item count [%s, %s] cached", dn_parkey, count));
		if(this.dnPartitionItemCountMap.size() >= cacheSize) {
			m_logger.debug("DN_Parkey item count cache is full, clean half size of the cache.");
			m_logger.debug(String.format("The hit ratio is %s. ", (double)cacheHit/totalQuery));
			cacheHit = 0;
			totalQuery = 0;
			// sort the list and flush half size of it
			ArrayList<String> dnparkeyList = new ArrayList<String>();
			for(String dnparkey: this.dnPartitionItemCountMap.keySet()) {
				dnparkeyList.add(dnparkey);
			}
			java.util.Collections.sort(dnparkeyList, new DNParkeyComparator());
			for(int i=cacheSize-1; i>=cacheSize/2; i--) {
				String dnparkey = dnparkeyList.get(i);
				this.dnPartitionItemCountMap.remove(dnparkey);
				this.dnPartitionItemCountTTL.remove(dnparkey);
			}
		}
		
		this.dnPartitionItemCountMap.put(dn_parkey, count);
		this.dnPartitionItemCountTTL.put(dn_parkey, current);
		
	}
	
	private synchronized void updateDNPartitionItemCount(String dn_parkey, long count) {
		if(this.dnPartitionItemCountMap.keySet().contains(dn_parkey)) {
			m_logger.debug("Update DN_Parkey item count cache");
			this.dnPartitionItemCountMap.replace(dn_parkey, count);
		}
	}
	
	private synchronized void removeDNPartitoinItemCount(String dn_parkey){
		this.dnPartitionItemCountMap.remove(dn_parkey);
		this.dnPartitionItemCountTTL.remove(dn_parkey);
	}
	
	public  class DNParkeyComparator implements Comparator<String> {

		@Override
		public int compare(String dnparkey1, String dnparkey2) {
			long current = System.currentTimeMillis();
			long ttl1 = current - dnPartitionItemCountTTL.get(dnparkey1);
			long ttl2 = current - dnPartitionItemCountTTL.get(dnparkey2);
			if(ttl1 == ttl2) {
				return 0;
			} else {
				if(ttl1 < ttl2) {
					return -1;
				}
				else 
					return 1;
			}
		}
		
	}

	@Override
	public synchronized void cleanNameNodeCache() throws NameNodeManagerException {
		m_logger.debug("Got a request to clean cache.");
		m_logger.debug(String.format("The hit ratio is %s. ", (double)cacheHit/totalQuery));
		this.dnPartitionItemCountMap.clear();
		this.dnPartitionItemCountTTL.clear();
	}
	
	public void getPartitionWithNoActivieDN() throws NameNodeManagerException{
		Connection conn = null;
		try{
			conn = m_dataSource.getConnection();
			Statement st = conn.createStatement();
			String sql = null;
			
			// load all DN partition relationships
			HashMap<Long, HashMap<String, Integer>> dnParRel = new HashMap<Long, HashMap<String, Integer>>();
			sql = "SELECT DN_KEY, PARTITION_ID, VERSION FROM T_DN_PARTITION";
			st.execute(sql);
			ResultSet rs = st.getResultSet();
			while(rs.next()){
				long partitionId = rs.getLong("PARTITION_ID");
				String dnKey = rs.getString("DN_KEY");
				int version = rs.getInt("VERSION");
				
				HashMap<String, Integer> dnParInfo = dnParRel.get(partitionId);
				if(dnParInfo == null){
					dnParInfo = new HashMap<String, Integer>();
					dnParRel.put(partitionId, dnParInfo);
				}
				dnParInfo.put(dnKey, version);
			}
			System.out.println(String.format("Finish loading %s data node partition relationships.", dnParRel.keySet().size()));
			
			// load all active DNs
			ArrayList<String> dnList = new ArrayList<String>();
			sql = "SELECT DN_KEY, DAEMON_IP FROM T_DATANODE";
			st.execute(sql);
			rs = st.getResultSet();
			while(rs.next()){
				String ip = rs.getString("DAEMON_IP");
				if(ip.equals("192.168.2.114") || ip.equals("192.168.2.105") || ip.equals("192.168.2.110"))
					continue;
				dnList.add(rs.getString("DN_KEY"));
			}
			
			// load all the partitions
			HashMap<Long, Integer> partitionList = new HashMap<Long, Integer>();
			sql = "SELECT PARTITION_ID, VERSION FROM T_PARTITION";
			st.execute(sql);
			rs = st.getResultSet();
			while(rs.next()){
				partitionList.put(rs.getLong("PARTITION_ID"), rs.getInt("VERSION"));
			}
			
			// test if partition is active
			ArrayList<Long> unActivePartitionList = new ArrayList<Long>();
			for (Long partitionId: dnParRel.keySet()){
				HashMap<String, Integer> dnParInfo = dnParRel.get(partitionId);
				int activeDNcount = 0;
				for(String dn: dnParInfo.keySet()){
					if(!dnList.contains(dn)){
						continue;
					}
					int dnVersion = dnParInfo.get(dn);
					int parVersion = partitionList.get(partitionId);
					if(dnVersion != parVersion){
						continue;
					}
				}
				if(activeDNcount < 2){
					unActivePartitionList.add(partitionId);
				}
			}
			
			
		} catch(SQLException e){
			throw new NameNodeManagerException(e);
		}
	}
}
