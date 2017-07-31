package com.cic.textengine.repository.importer;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.StringComparator;

public class JDBMManager {
	private static JDBMManager jdbmMan = null;
	private static String databaseName = null;
	private RecordManager recMan = null;
	private static Logger m_logger = Logger.getLogger(JDBMManager.class);
	private JDBMManager(String dbName) throws IOException{
		// init database instance
		Properties props = new Properties();
		props.setProperty(RecordManagerOptions.AUTO_COMMIT, "true");
		recMan = RecordManagerFactory.createRecordManager(
				dbName, props);
	}

	public BTree getBTree(String tableName) throws IOException {
		BTree tree = null;
		long recid = recMan.getNamedObject(tableName);
		if (recid != 0) {
			tree = BTree.load(recMan, recid);
		} else {
			tree = BTree.createInstance(recMan, new StringComparator());
			recMan.setNamedObject(tableName, tree.getRecid());
			recMan.commit();
		}

		return tree;

	}
	public static JDBMManager getInstance(String dbName) throws IOException{
		if (databaseName == null) {
			databaseName = dbName;
			jdbmMan = new JDBMManager(dbName);
			m_logger.debug("Create JDBM database " + dbName);
		}
		if (!databaseName.equalsIgnoreCase(dbName)) {
			throw new IOException("Not able to create two JDMB database within one process.");
		}
		return jdbmMan;
	}

	public BTree resetTable(BTree table, String tableName) throws IOException {
		recMan.delete(table.getRecid());
		table = BTree.createInstance(recMan, new StringComparator());
		recMan.setNamedObject(tableName, table.getRecid());
		recMan.commit();
		return table;
	}

	public synchronized void close() throws IOException {
		recMan.commit();
		databaseName = null;
		recMan.close();
		recMan = null;
	}
	
	public synchronized void remove(BTree table, Object key) throws IOException {
		table.remove(key);
	}

	public synchronized void commit() throws IOException {
		recMan.commit();
	}
}
