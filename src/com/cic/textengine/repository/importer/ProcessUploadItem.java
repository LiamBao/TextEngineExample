package com.cic.textengine.repository.importer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import org.apache.commons.codec.DecoderException;
import org.apache.log4j.Logger;

import com.cic.textengine.repository.ItemImporter;
import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.RemoteTEItemWriter;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemWriterException;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.datanode.type.PartitionRecord;
import com.cic.textengine.repository.exception.ItemImporterException;
import com.cic.textengine.repository.importer.exception.ProcessUploadItemException;
import com.cic.textengine.repository.importer.exception.UploadTEItemNotMatchLocalTEItemException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.tefilter.TEItemFilter;
import com.cic.textengine.tefilter.TEItemFilterFactory;
import com.cic.textengine.tefilter.exception.TEItemFilterException;
import com.cic.textengine.tefilter.queue.Queue;
import com.cic.textengine.tefilter.queue.TEFilterQueueFactory;
import com.cic.textengine.tefilter.queue.exception.TEFilterQueueException;
import com.cic.textengine.type.TEItem;

/**
 * This class is used to write local idf file to the remote datanode. It starts
 * 1 read thread and several write threads. The table used to store the
 * partition key/startItemID/itemCount called "writerKey", the format of data in
 * table like this key/value, the key is like this:
 * FF_281_2004_46494439325f33464944_FID92_3FID 43 the string before blank is the
 * partition key, and the number after blank is the partition's startItemID the
 * value is like this: 21, this number represents the item count of the
 * partition
 * 
 * 
 * @author ellen
 * 
 */
public class ProcessUploadItem implements ImporterProcess {
	private static Logger m_logger = Logger.getLogger(ProcessUploadItem.class);
	private static int MAX_NUM_OF_UPLOAD_THREADS = 10;
	private static String NNDAEMON_ADDR = null;
	private static int NNDAEMON_PORT = 0;
	private static String LOCALKEY_REMOTEKEY_FILE = "localkey_remotekey.map";

	private RepositoryEngine repoEngine = null;
	private boolean m_terminate = false;
	private boolean m_failed = false;
	private JDBMManager jdbmMan = null;
	
	private ArrayList<TEItemFilter> filterList = null;
	private Queue filterResultQueue = null;
	
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private final int flushBufferSize = 50000;
	private int flushCount = 0;

	public ProcessUploadItem(RepositoryEngine repoEngine) {

		this.repoEngine = repoEngine;
		
	}

	/**
	 * start upload local RepositoryEngine to TextEngine
	 */
	public void process(ItemImporterPerformanceLogger perfLogger)
			throws ProcessUploadItemException {

		NNDAEMON_ADDR = Configurer.getNNDaemonHost();
		NNDAEMON_PORT = Configurer.getNNDaemonPort();
		
		m_logger.debug("Init JDBM");
		BTree parKeyBTree = null;
		BTree parKeyStartItemIDBTree = null;

		try {
			// init database instance
			jdbmMan = JDBMManager.getInstance(ItemImporter.DATABASE);
			parKeyBTree = jdbmMan.getBTree(ItemImporter.BTREE_PARTITION_KEY);
			parKeyStartItemIDBTree = jdbmMan
					.getBTree(ItemImporter.BTREE_PARTITION_KEY_STARTITEMID);
		} catch (IOException e) {
			throw new ProcessUploadItemException(e);
		}

		if (parKeyBTree.size() <= 0) {// No partition to be uploaded.
			// close JDBM
			try {
				jdbmMan.close();
			} catch (IOException e) {
				m_logger.error("Error closing JDBM");
			}
			
			m_logger.info("No partition to be uploaded.");

			try {
				Thread.sleep(10000);
			} catch (InterruptedException e3) {
				// ignore
			}

			return;
		}
		
		
		try {
			this.initPrinter();
		} catch (IOException e4) {
			m_logger.error("Error init the file printer.");
			throw new ProcessUploadItemException(e4);
		}
		
		filterResultQueue = TEFilterQueueFactory.getInstance().getTEFilterQueue();
		try {
			filterResultQueue.init();
		} catch (TEFilterQueueException e1) {
			m_logger.error("Error in init the filter result queue: ");
			m_logger.error(e1.getLocalizedMessage());
			throw new ProcessUploadItemException(e1);
		}
		
		ArrayList<String> filterName = Configurer.getFilterNames();
		filterList = new ArrayList<TEItemFilter>();
		
		for(String name : filterName) {
			try {
				TEItemFilter filter = TEItemFilterFactory.getInstance().getFilter(name);
				filter.init();
				filterList.add(filter);
			} catch (TEItemFilterException e) {
				m_logger.error("Fail to add filter: "+name);
				m_logger.error(e.getLocalizedMessage());
			}
		}

		// init the itemIDLogger
		// UploadedItemIDLogger itemIDLogger = null;
		// try {
		// itemIDLogger = new UploadedItemIDLogger();
		// } catch (FileNotFoundException e1) {
		// throw new ProcessParseXMLException(e1);
		// }

		m_failed = false;

		ExecutorService exec = Executors
				.newFixedThreadPool(MAX_NUM_OF_UPLOAD_THREADS);

		try {
			Tuple tuple = new Tuple();
			TupleBrowser btree_browser;
			btree_browser = parKeyBTree.browse();

			while (btree_browser.getNext(tuple) && !this.m_terminate) {
				String parkey = tuple.getKey().toString();
				Long itemCount = (Long) (tuple.getValue());
				// m_logger.debug("Start uploading thread for [ParKey:" + parkey
				// + ",ItemCount:" + itemCount + "]");

				exec.submit(new PartitionUploadRunnable(this.repoEngine,
						parkey, itemCount.longValue(), parKeyBTree,
						parKeyStartItemIDBTree, null, perfLogger));
			}

			exec.shutdown();
			while (!exec.isTerminated()) {
				exec.awaitTermination(86400, TimeUnit.SECONDS);
			}

			// close itemIDLogger
			// itemIDLogger.close();
		} catch (IOException e) {
			throw new ProcessUploadItemException(e);
		} catch (InterruptedException e1) {
			throw new ProcessUploadItemException(e1);
		} finally {
			if(parKeyBTree.size()>0){
				this.m_failed = true;
				m_logger.error("More partitions to be uploaded.");
			}
			// close JDBM finally
			try {
				jdbmMan.close();
			} catch (IOException e2) {
				// ignore this exception
			}
			
			for(TEItemFilter filter: filterList) {
				try {
					filter.close();
				} catch (TEItemFilterException e3) {
					m_logger.error("Error close the filter.");
					m_logger.error(e3.getLocalizedMessage());
				}
			}
			filterList.clear();
			filterList = null;
			
			try {
				filterResultQueue.close();
			} catch (TEFilterQueueException e1) {
				m_logger.error("Error close the filter result queue.");
				m_logger.error(e1.getLocalizedMessage());
			}
			filterResultQueue = null;
			this.closePrinter();
			
		}

		if (this.m_failed) {
			throw new ProcessUploadItemException(
					"Failed to upload all items to TextEngine.");
		}
	}

	public boolean isFailed() {
		return m_failed;
	}

	class UploadedItemIDLogger {
		String FILENAME_TEMP_LOG = "ItemIDLogger.tmp";

		PrintWriter m_printWriter = null;

		public UploadedItemIDLogger() throws FileNotFoundException {
			m_printWriter = new PrintWriter(new FileOutputStream(
					FILENAME_TEMP_LOG, true));
		}

		public synchronized void log(String parKey, long startItemID, int count) {
			m_printWriter.println(parKey + "," + startItemID + "," + count);
			m_printWriter.flush();
		}

		public void close() {
			m_printWriter.flush();
			m_printWriter.close();
			File f = new File(FILENAME_TEMP_LOG);
			f.renameTo(new File("ItemIDLogger_" + System.currentTimeMillis()
					+ ".iid"));
		}
	}

	/**
	 * The thread body which is used to upload the data to the Text Repository
	 * 
	 * @author denis.yu
	 * 
	 */
	class PartitionUploadRunnable implements Runnable {
		private String m_ParKey = null;
		private RepositoryEngine m_RepoEngine = null;
		private long m_itemCount = 0;
		private BTree m_parKeyBTree = null;
		private BTree m_parKeyStartItemIDBTree = null;
		// private UploadedItemIDLogger m_itemIDLogger = null;
		private ItemImporterPerformanceLogger m_perfLogger = null;

		PartitionUploadRunnable(RepositoryEngine repoEngine, String strParKey,
				long itemCount, BTree parKeyBTree,
				BTree parKeyStartItemIDBTree,
				UploadedItemIDLogger itemIDLogger,
				ItemImporterPerformanceLogger perfLogger) {
			m_ParKey = strParKey;
			m_RepoEngine = repoEngine;
			m_itemCount = itemCount;
			m_parKeyBTree = parKeyBTree;
			m_parKeyStartItemIDBTree = parKeyStartItemIDBTree;
			// m_itemIDLogger = itemIDLogger;
			m_perfLogger = perfLogger;
		}

		public void run() {
			try {
				long timestamp_start = System.currentTimeMillis();
				long itemcount = addIDF2DataNode(m_ParKey, m_RepoEngine,
						m_parKeyStartItemIDBTree);
				this.m_perfLogger.logItemUploaderPerformance(itemcount,
						timestamp_start);
			} catch (ItemImporterException e) {
				m_logger.error("Failed to upload the partition [key:"
						+ m_ParKey, e);
				m_failed = true;
				return;
			} catch (DecoderException e) {
				m_logger.error("Failed to upload the partition [key:"
						+ m_ParKey, e);
				m_failed = true;
				return;
			} catch (NameNodeClientException e) {
				m_logger.error("Failed to upload the partition [key:"
						+ m_ParKey, e);
				m_failed = true;
				return;
			} catch (IOException e) {
				m_logger.error("Failed to upload the partition [key:"
						+ m_ParKey, e);
				m_failed = true;
				return;
			} catch (RepositoryEngineException e) {
				m_logger.error("Failed to upload the partition [key:"
						+ m_ParKey, e);
				m_failed = true;
				return;
			} catch (DataNodeClientException e) {
				m_logger.error("Failed to upload the partition [key:"
						+ m_ParKey, e);
				m_failed = true;
				return;
			} catch (DataNodeClientCommunicationException e) {
				m_logger.error("Failed to upload the partition [key:"
						+ m_ParKey, e);
				m_failed = true;
				return;
			}

			// remove the partition key from the database.
			try {
				this.m_parKeyBTree.remove(m_ParKey);
				jdbmMan.commit();
			} catch (IOException e) {
				m_logger.fatal("Failed to erase the partition key from DB.");
				m_failed = true;
				m_terminate = true;
				return;
			}
		}

		long addIDF2DataNode(String key, RepositoryEngine repoEngine,
				BTree parKeyStartItemIDBTree) throws ItemImporterException,
				DecoderException, NameNodeClientException, IOException,
				RepositoryEngineException, DataNodeClientException,
				DataNodeClientCommunicationException {

			long startdt = System.currentTimeMillis();

			RemoteTEItemWriter itemWriter = null;
			NameNodeClient nn_client = new NameNodeClient(NNDAEMON_ADDR,
					NNDAEMON_PORT);
			DataNodeClient dn_client = null;

			PartitionKey parkey = null;
			parkey = PartitionKey.decodeStringKey(key);

			int year = parkey.getYear();
			int month = parkey.getMonth();
			String siteid = parkey.getSiteID();
			String forumid = parkey.getForumID();

			dn_client = nn_client.getDNClientForWriting(year, month, siteid,
					forumid);

			itemWriter = dn_client.getWriter(year, month, siteid, forumid);

			itemWriter.writeLocalPartition(repoEngine, m_itemCount);

			itemWriter.flush();
			try {
				itemWriter.close();
			} catch (RemoteTEItemWriterException e1) {
				throw new ItemImporterException(e1);
			}
			m_logger.debug("Patition is uploaded to TR [startItemID:"
					+ itemWriter.getStartItemID() + ",ItemCount:"
					+ itemWriter.getCount() + "]");

			if (itemWriter.getCount() != m_itemCount) {
				m_logger.fatal("Number of uploaded items are not matching: [y:"
						+ year + ",m:" + month + ",s:" + siteid + ",f:"
						+ forumid + ", NumOfLocalItems:" + m_itemCount
						+ ", NumOfWrittenItems:" + itemWriter.getCount()
						+ ",dn: " + dn_client.getHost() + "] ");
				throw new ItemImporterException(
						"Number of uploaded items are not matching: [y:" + year
								+ ",m:" + month + ",s:" + siteid + ",f:"
								+ forumid + ", NumOfLocalItems:" + m_itemCount
								+ ", NumOfWrittenItems:"
								+ itemWriter.getCount() + ",dn: "
								+ dn_client.getHost() + "] ");
			}
			
			long startItemID = itemWriter.getStartItemID();
			int count = 0;
			PartitionEnumerator enu = repoEngine.getPartitionEnumerator(year,
					month, siteid, forumid);
			if(siteid.startsWith("TM")) {
				tmItemInfoOutput(enu, startItemID);
			}
			while (enu.next()) {
				TEItem item = enu.getItem();
				for (TEItemFilter filter : filterList) {
					if (filter.accept(item)) {
						ItemKey itemkey = new ItemKey(item.getMeta()
								.getSource(), String.valueOf(item.getMeta()
								.getSiteID()), item.getMeta().getForumID(),
								item.getMeta().getYearOfPost(), item.getMeta()
										.getMonthOfPost(), startItemID + count);
						try {
							filterResultQueue.put(filter.getFilterName(),
									itemkey.generateKey());
						} catch (TEFilterQueueException e) {
							m_logger
									.error(String
											.format(
													"Error insert into filter queue: ItemKey: [%s], FilterName: [%s]",
													itemkey.generateKey(),
													filter.getFilterName()));
						}
					}
				}
				count++;
			}
			enu.close();

			m_logger.info("Verify the updated partition data.");
//			verifyUploadedData(repoEngine, itemWriter, nn_client, year, month,
//					siteid, forumid);

			// m_itemIDLogger.log(key, itemWriter.getStartItemID(),
			// itemWriter.getCount());

			/*
			 * Record those items of one partition which were succesfully
			 * uploaded into database, Later process may exploit these
			 * information.
			 */
			PartitionRecord record = new PartitionRecord(itemWriter
					.getStartItemID(), itemWriter.getCount());
			parKeyStartItemIDBTree.insert(key, record, true);

			m_logger.info("Parition uploaded [y:" + year + ",m:" + month
					+ ",s:" + siteid + ",f:" + forumid + "] "
					+ itemWriter.getCount() + " items in "
					+ (System.currentTimeMillis() - startdt) + "ms");

			return itemWriter.getCount();
		}

		private void verifyUploadedData(RepositoryEngine repoEngine,
				RemoteTEItemWriter itemWriter, NameNodeClient nn_client,
				int year, int month, String siteid, String forumid)
				throws NameNodeClientException, DataNodeClientException,
				DataNodeClientCommunicationException,
				RepositoryEngineException, IOException, ItemImporterException {
			DataNodeClient dn_client;
			dn_client = nn_client.getDNClientForQuery(year, month, siteid,
					forumid);
			RemoteTEItemEnumerator trEnu = dn_client.getItemEnumerator(year,
					month, siteid, forumid, itemWriter.getStartItemID(),
					itemWriter.getCount(), true);

			PartitionEnumerator localEnu = repoEngine.getPartitionEnumerator(
					year, month, siteid, forumid);

			TEItem localItem = null;
			TEItem remoteItem = null;
			int count = 0;
			while (localEnu.next()) {
				count++;
				if (trEnu.next()) {
					localItem = localEnu.getItem();
					remoteItem = trEnu.getItem();

					// compare the two objects
					try {
						compareTEItem(localItem, remoteItem);
					} catch (UploadTEItemNotMatchLocalTEItemException e) {
						// this should never happen.
						m_logger
								.fatal("Uploaded item and local item are not identical: [y:"
										+ year
										+ ",m:"
										+ month
										+ ",s:"
										+ siteid
										+ ",f:"
										+ forumid
										+ ", iid: "
										+ remoteItem.getMeta().getItemID()
										+ "] ");
						localEnu.close();
						trEnu.close();
						throw new ItemImporterException(e);
					}
				} else {
					m_logger
							.fatal("Numer of uploaded items is less than the local items: [y:"
									+ year
									+ ",m:"
									+ month
									+ ",s:"
									+ siteid
									+ ",f:"
									+ forumid
									+ ",NumOfRemoteItems:"
									+ (count - 1)
									+ ",NumOfLocalItems:"
									+ m_itemCount + "] ");
					localEnu.close();
					trEnu.close();
					throw new ItemImporterException(
							"Numer of uploaded items is less than the local items: [y:"
									+ year + ",m:" + month + ",s:" + siteid
									+ ",f:" + forumid + ",NumOfRemoteItems:"
									+ (count - 1) + ",NumOfLocalItems:"
									+ m_itemCount + "] ");
				}
			}

			localEnu.close();
			trEnu.close();
			if (count != itemWriter.getCount()) {
				m_logger
						.fatal("Number of uploaded items doesn't match local items: [y:"
								+ year
								+ ",m:"
								+ month
								+ ",s:"
								+ siteid
								+ ",f:"
								+ forumid
								+ ",ItemCountWritten:"
								+ itemWriter.getCount()
								+ ",ItemCountLocal:"
								+ count + "] ");
				throw new ItemImporterException(
						"Number of uploaded items doesn't match local items: [y:"
								+ year + ",m:" + month + ",s:" + siteid + ",f:"
								+ forumid + ",ItemCountWritten:"
								+ itemWriter.getCount() + ",ItemCountLocal:"
								+ count + "] ");
			}
		}
	}

	/**
	 * Compare if two items are same, except the itemID field.
	 * 
	 * @param localItem
	 * @param remoteItem
	 * @return
	 */
	void compareTEItem(TEItem localItem, TEItem remoteItem)
			throws UploadTEItemNotMatchLocalTEItemException {
		if (!localItem.getSubject().equals(remoteItem.getSubject())) {
			throw new UploadTEItemNotMatchLocalTEItemException("subject",
					remoteItem.getSubject(), localItem.getSubject());
		} else if (!localItem.getContent().equals(remoteItem.getContent())) {
			throw new UploadTEItemNotMatchLocalTEItemException("content",
					remoteItem.getContent(), localItem.getContent());
		} else if (localItem.getMeta().getDateOfPost() != remoteItem.getMeta()
				.getDateOfPost()) {
			throw new UploadTEItemNotMatchLocalTEItemException("dateofpost",
					Long.toString(remoteItem.getMeta().getDateOfPost()), Long
							.toString(localItem.getMeta().getDateOfPost()));
		} else if (!localItem.getMeta().getForumID().equals(
				remoteItem.getMeta().getForumID())) {
			throw new UploadTEItemNotMatchLocalTEItemException("forumid",
					remoteItem.getMeta().getForumID(), localItem.getMeta()
							.getForumID());
		} else if (!localItem.getMeta().getForumName().equals(
				remoteItem.getMeta().getForumName())) {
			throw new UploadTEItemNotMatchLocalTEItemException("forumname",
					remoteItem.getMeta().getForumName(), localItem.getMeta()
							.getForumName());
		} else if (!localItem.getMeta().getItemType().equals(
				remoteItem.getMeta().getItemType())) {
			throw new UploadTEItemNotMatchLocalTEItemException("itemtype",
					remoteItem.getMeta().getItemType(), localItem.getMeta()
							.getItemType());
		} else if (!localItem.getMeta().getKeyword().equals(
				remoteItem.getMeta().getKeyword())) {
			throw new UploadTEItemNotMatchLocalTEItemException("keyword",
					remoteItem.getMeta().getKeyword(), localItem.getMeta()
							.getKeyword());
		} else if (!localItem.getMeta().getKeywordGroup().equals(
				remoteItem.getMeta().getKeywordGroup())) {
			throw new UploadTEItemNotMatchLocalTEItemException("keywordgroup",
					remoteItem.getMeta().getKeywordGroup(), localItem.getMeta()
							.getKeywordGroup());
		} else if (!localItem.getMeta().getPoster().equals(
				remoteItem.getMeta().getPoster())) {
			throw new UploadTEItemNotMatchLocalTEItemException("poster",
					remoteItem.getMeta().getPoster(), localItem.getMeta()
							.getPoster());
		} else if (!localItem.getMeta().getPosterID().equals(
				remoteItem.getMeta().getPosterID())) {
			throw new UploadTEItemNotMatchLocalTEItemException("posterid",
					remoteItem.getMeta().getPosterID(), localItem.getMeta()
							.getPosterID());
		} else if (!localItem.getMeta().getPosterUrl().equals(
				remoteItem.getMeta().getPosterUrl())) {
			throw new UploadTEItemNotMatchLocalTEItemException("posterurl",
					remoteItem.getMeta().getPosterUrl(), localItem.getMeta()
							.getPosterUrl());
		} else if (localItem.getMeta().getSiteID() != remoteItem.getMeta()
				.getSiteID()) {
			throw new UploadTEItemNotMatchLocalTEItemException("siteid", Long
					.toString(remoteItem.getMeta().getSiteID()), Long
					.toString(localItem.getMeta().getSiteID()));
		} else if (!localItem.getMeta().getSiteName().equals(
				remoteItem.getMeta().getSiteName())) {
			throw new UploadTEItemNotMatchLocalTEItemException("sitename",
					remoteItem.getMeta().getSiteName(), localItem.getMeta()
							.getSiteName());
		} else if (!localItem.getMeta().getSource().equals(
				remoteItem.getMeta().getSource())) {
			throw new UploadTEItemNotMatchLocalTEItemException("source",
					remoteItem.getMeta().getSource(), localItem.getMeta()
							.getSource());
		} else if (localItem.getMeta().getThreadID() != remoteItem.getMeta()
				.getThreadID()) {
			throw new UploadTEItemNotMatchLocalTEItemException("threadid", Long
					.toString(remoteItem.getMeta().getThreadID()), Long
					.toString(localItem.getMeta().getThreadID()));
		} else if (!localItem.getMeta().getForumUrl().equals(
				remoteItem.getMeta().getForumUrl())) {
			throw new UploadTEItemNotMatchLocalTEItemException("forumurl",
					remoteItem.getMeta().getForumUrl(), localItem.getMeta()
							.getForumUrl());
		} else if (!localItem.getMeta().getItemUrl().equals(
				remoteItem.getMeta().getItemUrl())) {
			throw new UploadTEItemNotMatchLocalTEItemException("itemurl",
					remoteItem.getMeta().getItemUrl(), localItem.getMeta()
							.getItemUrl());
		}
	}
	
	private void tmItemInfoOutput(PartitionEnumerator enu, long startItemID) throws IOException, RepositoryEngineException {

		int count = 0;
		String siteid = null;
		String source = null;
		String forumid = null;
		int year = 0;
		int month = 0;
		
		while(enu.next()) {
			TEItem item = enu.getItem();
			long localItemID = item.getMeta().getItemID();
			long remoteItemID = startItemID + count;
			count ++;
			siteid = Long.toString(item.getMeta().getSiteID());
			source = item.getMeta().getSource();
			forumid = item.getMeta().getForumID();
			year = item.getMeta().getYearOfPost();
			month = item.getMeta().getMonthOfPost();
			ItemKey localItemkey = new ItemKey(source, siteid, forumid, year, month, localItemID);
			ItemKey remoteItemkey = new ItemKey(source, siteid, forumid, year, month, remoteItemID);
			addItemKeyMap(localItemkey.generateKey(), remoteItemkey.generateKey());

		}
	}
	
	private synchronized void addItemKeyMap(String localItemKey, String remoteItemkey) {
		String outStr = localItemKey + ","  + remoteItemkey;
		pw.println(outStr);
		this.flushCount ++;
		if(this.flushCount == this.flushBufferSize) {
			pw.flush();
			this.flushCount = 0;
		}
	}
	
	private void initPrinter() throws IOException {
		this.fw = new FileWriter(LOCALKEY_REMOTEKEY_FILE, true);
		this.pw = new PrintWriter(fw);
		this.flushCount = 0;
	}
	
	private void closePrinter() {
		this.flushCount = 0;
		this.pw.close();
		this.pw = null;
		try {
			this.fw.close();
		} catch (IOException e) {
			m_logger.error("Error close the file writer!");
		}
		this.fw = null;
	}
	
	public static void main(String[] args) throws RepositoryEngineException, ProcessUploadItemException, IOException {
		Configurer.config("ItemImporter.properties");
		File file = new File("D:\\TERepo");
		RepositoryEngine m_localRepoEngine =RepositoryFactory.getNewRepositoryEngineInstance(file.getAbsolutePath());
		ProcessUploadItem process = new ProcessUploadItem(m_localRepoEngine);
		process.process(new ItemImporterPerformanceLogger());
	}

}