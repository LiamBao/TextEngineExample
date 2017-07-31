package com.cic.textengine.repository;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import org.apache.log4j.Logger;

import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.exception.ItemImporterException;
import com.cic.textengine.repository.importer.ImporterProcess;
import com.cic.textengine.repository.importer.ItemImporterPerformanceLogger;
import com.cic.textengine.repository.importer.ProcessDataConsolidate;
import com.cic.textengine.repository.importer.ProcessDeleteItem;
import com.cic.textengine.repository.importer.ProcessFinishXML;
import com.cic.textengine.repository.importer.ProcessIsolateXML;
import com.cic.textengine.repository.importer.ProcessParseXML;
import com.cic.textengine.repository.importer.ProcessUpdatePosttrend;
import com.cic.textengine.repository.importer.ProcessUploadItem;
import com.cic.textengine.repository.importer.exception.ImporterProcessException;
import com.cic.textengine.repository.importer.exception.ProcessParseXMLException;
import com.cic.textengine.utils.BloomFilterHelper;

/**
 * This class is used to work as a daemon to continues import data into Text
 * Repository
 * 
 * @author denis.yu
 * 
 * 
 * 
 */

public class ItemImporter implements Runnable {
	static Logger m_logger = Logger.getLogger(ItemImporter.class);

	File m_XMLFolder_Source = null;
	File m_XMLFolder_Isolation = null;
	File m_XMLFolder_Finished = null;
	File m_XMLFoler_Corrupted = null;

	File db_env_file = null;

	// the name of jdbm db file
	public static String DATABASE = "ItemImporterDB";
	// the name of table which stores the partition key
	public static String BTREE_PARTITION_KEY = "PatitionRecord";

	public static String BTREE_PARTITION_KEY_STARTITEMID = "PatitionStartRecord";

	public final static String FILENAME_PROCESS_STATUS = "ItemImporterProcess.temp";

	public static final String ITEM_IMPORTER_PROPERTIES = "ItemImporter.properties";

	// the name of table to store the index of current item which has been
	// translate to idf
	static String BTREE_ITEMID = "IndexRecord";

	static String IDF_TABLE = "IDFRecord";
	
	// the tcp port listening signal to stop the itemimporter
	public final static int DAEMON_PORT = 9876;
	public final static int STOP_SIGNAL = 0;
	
	// the max amount of xml files can be moved into processing folder

	RepositoryEngine m_localRepoEngine = null;

	boolean m_stop = false;
	Thread m_thread = null;

	ArrayList<ImporterProcess> m_processList = new ArrayList<ImporterProcess>();

	public ItemImporter(File sourceFolder) throws ItemImporterException {

		m_XMLFolder_Source = sourceFolder;

		if (!(m_XMLFolder_Source.exists() && m_XMLFolder_Source.isDirectory())) {
			throw new ItemImporterException("Source folder does not exists:["
					+ m_XMLFolder_Source.getAbsolutePath() + "]");
		}

		m_XMLFolder_Finished = new File("Finished");
		if (!(m_XMLFolder_Finished.exists() && m_XMLFolder_Finished
				.isDirectory())) {
			m_XMLFolder_Finished.mkdir();
		}

		// init the folder for storing processing XML. This folder MUST under
		// source folder because XML files will be moved from source folder to
		// this folder for futher processing.
		m_XMLFolder_Isolation = new File(
				"Isolation");
		
		if (!(m_XMLFolder_Isolation.exists() && m_XMLFolder_Isolation
				.isDirectory())) {
			m_XMLFolder_Isolation.mkdir();
		}
		
		// init the folder for storing corrupted XML files which are found during the XML parse process.
		m_XMLFoler_Corrupted = new File("Corrupted");
		
		if (!(m_XMLFoler_Corrupted.exists() && m_XMLFoler_Corrupted
				.isDirectory())) {
			m_XMLFoler_Corrupted.mkdir();
		}

		//File file = new File(m_XMLFolder_Source.getAbsolutePath()
		//		+ File.separator + "TERepo");
		File file = new File("TERepo");
		if (!(file.exists() && file.isDirectory())) {
			file.mkdir();
		}

		try {
			this.m_localRepoEngine = RepositoryFactory
					.getNewRepositoryEngineInstance(file.getAbsolutePath());
		} catch (RepositoryEngineException e) {
			throw new ItemImporterException(e);
		}

		// init the process list
		ImporterProcess process = null;
		process = new ProcessIsolateXML(this.m_XMLFolder_Source,
				this.m_XMLFolder_Isolation);
		m_processList.add(process);

		try {
			process = new ProcessParseXML(this.m_XMLFolder_Isolation, this.m_XMLFoler_Corrupted,
					this.m_localRepoEngine);
		} catch (ProcessParseXMLException e) {
			throw new ItemImporterException(e);
		}
		m_processList.add(process);

		process = new ProcessUploadItem(this.m_localRepoEngine);
		m_processList.add(process);

		process = new ProcessUpdatePosttrend();
		m_processList.add(process);
		
		process = new ProcessDataConsolidate(this.m_XMLFolder_Isolation);
		m_processList.add(process);
		
		process = new ProcessDeleteItem();
		m_processList.add(process);
		
		process = new ProcessFinishXML(this.m_XMLFolder_Isolation,
				this.m_XMLFolder_Finished);
		m_processList.add(process);
	}

	public void start() {
		
		ItemImporterDaemon daemon = new ItemImporterDaemon();
		daemon.start();
		
		m_stop = false;
		m_thread = new Thread(this);
		m_thread.start();
	}

	public void stop() throws InterruptedException {
		m_stop = true;
		m_thread.join();
	}

	public void waitUntilFinish() throws InterruptedException {
		m_thread.join();
	}

	public void run() {

		ItemImporterPerformanceLogger perfLogger = null;
		try {
			perfLogger = new ItemImporterPerformanceLogger();
		} catch (IOException e3) {
			m_logger
					.error("Error initing performance logger, halt the process.");
			return;
		}

		int processIdx = 0;
		ImporterProcess process = null;

		m_logger.info("Load previous running status...");
		try {
			Properties properties = new Properties();
			FileInputStream fis = new FileInputStream(FILENAME_PROCESS_STATUS);
			properties.load(fis);
			fis.close();

			Enumeration<Object> enu = properties.keys();
			if (enu.hasMoreElements()) {
				processIdx = Integer.parseInt((String) (enu.nextElement()));
			}
		} catch (FileNotFoundException e2) {
			// ignore
		} catch (IOException e2) {
			m_logger.error("Error resuming previous running status.");
			return;
		}

		if (processIdx > 0) {
			m_logger.info("Resume process from step: " + processIdx);
		}

		perfLogger.startSession();

		while (!m_stop) {
			process = m_processList.get(processIdx);
			try {
				FileOutputStream fos;
				try {
					Properties properties = new Properties();
					fos = new FileOutputStream(FILENAME_PROCESS_STATUS, false);
					properties.setProperty(Integer.toString(processIdx),
							"running");
					properties.store(fos, "ItemImporter process status");
					fos.close();
				} catch (FileNotFoundException e) {
					m_logger.error("Exception", e);
					return;
				} catch (IOException e) {
					m_logger.error("Exception", e);
					return;
				}

				process.process(perfLogger);
				processIdx++;
				if (processIdx >= m_processList.size()) {// start all over
					// again
					perfLogger.closeSession();
					processIdx = 0;
					perfLogger.startSession();
				}

			} catch (ImporterProcessException e) {
				m_logger.error("Exception", e);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					// ignore
				}
			}
		}
		
		FileOutputStream fos;
		try {
			Properties properties = new Properties();
			fos = new FileOutputStream(FILENAME_PROCESS_STATUS, false);
			properties.setProperty(Integer.toString(processIdx),
					"running");
			properties.store(fos, "ItemImporter process status");
			fos.close();
		} catch (FileNotFoundException e) {
			m_logger.error("Exception", e);
			return;
		} catch (IOException e) {
			m_logger.error("Exception", e);
			return;
		}

		try {
			BloomFilterHelper.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		m_logger.info("ItemImporter closed at "+new Date(System.currentTimeMillis()));
	}

	public static void main(String args[]) {
		if (args.length != 1) {
			System.out.println("One argument needed: srcDir ");
			return;
		}

		try {
			Configurer.config(ITEM_IMPORTER_PROPERTIES);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		File m_source = new File(args[0]);
		ItemImporter itemImporter = null;

		try {
			itemImporter = new ItemImporter(m_source);

			itemImporter.start();

			itemImporter.waitUntilFinish();

		} catch (ItemImporterException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	class ItemImporterDaemon implements Runnable {

		private boolean m_stop = false;
		private Thread m_thread = null;
		
		public void run() {
			ServerSocket server = null;
			try {
				server = new ServerSocket(ItemImporter.DAEMON_PORT);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				while(!m_stop)
				{
					Socket socket = server.accept();
					serveSocket(socket);
				}
			} catch (IOException e)
			{
				
			}
		}
		
		public void start()
		{
			m_thread = new Thread(this);
			m_thread.start();
		}
		
		private void serveSocket(Socket socket)
		{
			try {
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				if(dis.readInt() == STOP_SIGNAL)
				{
					stop();
					m_stop = true;
				}
				dis.close();
				socket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
