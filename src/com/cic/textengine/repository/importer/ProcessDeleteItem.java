package com.cic.textengine.repository.importer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import com.cic.common.service.database.DB;
import com.cic.textengine.datadelivery.DcmisDB;
import com.cic.textengine.posttrend.PostTrend;
import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.importer.exception.ImporterProcessException;
import com.cic.textengine.repository.importer.exception.ProcessDeletionItemException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.utils.BloomFilterHelper;

public class ProcessDeleteItem implements ImporterProcess {

	private static String NNDaemonAddr = null;;
	private static int NNDaemonPort = 0;

	private static Logger m_logger = Logger.getLogger(ProcessDeleteItem.class);
	
	private long totalItemCount = 0;
	private long startDT = 0;
	private ItemImporterPerformanceLogger m_perfLogger = null;
	
	public void process(ItemImporterPerformanceLogger perfLogger)
			throws ImporterProcessException {
		// obtain the current sub-process to resume

		NNDaemonAddr = Configurer.getNNDaemonHost();
		NNDaemonPort = Configurer.getNNDaemonPort();
		m_perfLogger = perfLogger;
		
		m_logger.info("Load current deletion operation...");
		startDT = System.currentTimeMillis();
		
		/*
		 * query DCMIS database for those in-progress deletion operations and
		 * obtain the parameters of these deletions
		 */
		ArrayList<DeleteCondition> deleteList = new ArrayList<DeleteCondition>();
		try {
			DB.createConnection();
			Connection ptconn = DB.getConnection();
			
			DcmisDB.createConnection();
			Connection conn = DcmisDB.getConnection();
			String sql = "select * from T_DELETION where STATUS = 1 and TYPE = 1";
			Statement stat = conn.createStatement();
			stat.execute(sql);
			ResultSet result = stat.getResultSet();
			while (result.next()) {
				String source = result.getString("SOURCE");
				String siteid = result.getString("SITE_ID");
				String forumid = result.getString("FORUM_ID");
				String threadid = result.getString("THREAD_ID");
				int year = result.getInt("YEAR");
				int month = result.getInt("MONTH");
				Date startdate = result.getDate("LAST_EXTRACTION_DATE1");
				Date enddate = result.getDate("LAST_EXTRACTION_DATE2");

				String siteStr = source+siteid;
				if(forumid == null){
					// Query the forum ID from the post trend DB
					ArrayList<String> forumidList = getForumID(ptconn, year, month, siteStr);
					for(String id: forumidList) {
						PartitionKey parkey = new PartitionKey(year, month, siteStr, id);
						String queryStr = generateQueryStr(source, siteid, id,
								threadid, year, month, startdate, enddate);
						deleteList.add(new DeleteCondition(parkey, queryStr, isCleanPartition(threadid, startdate)));
					}
				} else {
					PartitionKey parkey = new PartitionKey(year, month, siteStr, forumid);
					String queryStr = generateQueryStr(source, siteid, forumid,
							threadid, year, month, startdate, enddate);
					deleteList.add(new DeleteCondition(parkey, queryStr, isCleanPartition(threadid, startdate)));
				}
			}
			
			DB.close();

			Timestamp current = new Timestamp(System.currentTimeMillis());
			sql = "update T_DELETION set START_TIME = ? where STATUS = 1 and TYPE = 1";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setTimestamp(1, current);
			ps.execute();
		} catch (Exception e) {
			m_logger.error("Error in obtaining info from DCMIS db.");
			DcmisDB.close();
			throw new ProcessDeletionItemException(e);
		}
		DcmisDB.close();
		if (deleteList.size() <= 0) {
			m_logger.info("No deletion operation.");
			return;
		}
		
		PostTrend pt = new PostTrend();
		NameNodeClient nnClient = new NameNodeClient(NNDaemonAddr, NNDaemonPort);
		try {
			DB.createConnection();
			Connection conn = DB.getConnection();
			String sql = "select * from T_POSTTREND where year = ? and month = ? and site_id = ? and forum_id = ?";
			PreparedStatement ps = conn.prepareStatement(sql);
			int checkpoint = 0;
			int postCount = 0;
			for(DeleteCondition con: deleteList) {
				PartitionKey parkey = con.getParkey();
				int year = parkey.getYear();
				int month = parkey.getMonth();
				String siteid = parkey.getSiteID();
				String forumid = parkey.getForumID();
				postCount = 0;
				
				ps.setInt(1, year);
				ps.setInt(2, month);
				ps.setString(3, siteid);
				ps.setString(4, forumid);
				ps.execute();
				ResultSet rs = ps.getResultSet();
				if(rs.next()) {
					checkpoint = rs.getInt("checkpoint_itemid");
					postCount = rs.getInt("post_count");
				}
				int count = 0;
				if(postCount >0){
					if (con.isCleanPartition()) {
						// remove from bloom filter
						DataNodeClient dnClient = nnClient
								.getDNClientForQuery(parkey);
						RemoteTEItemEnumerator enu = null;
						if (checkpoint == 0)
							enu = dnClient.getItemEnumerator(year, month,
									siteid, forumid);
						else
							enu = dnClient.getItemEnumerator(year, month,
									siteid, forumid, checkpoint + 1, postCount
											- checkpoint, false);
						while (enu.next()) {
							TEItem item = enu.getItem();
							BloomFilterHelper.remove(parkey, item);
							count++;
						}
						enu.close();

						if (checkpoint == 0) {
							// clean the partition
							pt.cleanTrend(parkey);
							nnClient.cleanPartition(year, month, siteid,
									forumid);
						} else {
							// delete the un-consolidate item
							pt.setTrend(parkey, checkpoint);
							ArrayList<Long> itemid_list = new ArrayList<Long>();
							for (long i = checkpoint + 1; i <= postCount; i++)
								itemid_list.add(i);
							dnClient.deleteItems(year, month, siteid, forumid,
									itemid_list, true);
						}

					} else {
						// query items
						DataNodeClient dnClient = nnClient
								.getDNClientForWriting(year, month, siteid,
										forumid);
						ArrayList<TEItem> itemList = dnClient.queryItems(year,
								month, siteid, forumid, con.getCondition());
						ArrayList<Long> itemid_list = new ArrayList<Long>();

						count = itemList.size();
						if (count > 0) {

							// delete items from bloom filter
							for (TEItem item : itemList) {
								long itemid = item.getMeta().getItemID();
								if (itemid > checkpoint) {
									BloomFilterHelper.remove(parkey, item);
									itemid_list.add(itemid);
								} else {
									count--;
								}
							}
							con.setCount(count);
							// update post trend
							pt.deleteTrend(parkey, con.getCount());

							// delete items from data node
							dnClient.deleteItems(year, month, siteid, forumid,
									itemid_list, false);

							m_logger
									.debug(String
											.format(
													"%s items are deleted under the condition %s from partition [Y: %s, M:%s, S:%s, F:%s].",
													count, con.getCondition(),
													year, month, siteid,
													forumid));
						} else {
							m_logger
									.debug(String
											.format(
													"No item is found under the condition %s from partition [Y: %s, M:%s, S:%s, F:%s].",
													con.getCondition(), year,
													month, siteid, forumid));
						}
					}

					totalItemCount += count;
				}
			}
		} catch (NameNodeClientException e) {
			m_logger.error("Error in communication with Name Node.");
			DcmisDB.close();
			throw new ProcessDeletionItemException(e);
		} catch (DataNodeClientCommunicationException e) {
			m_logger.error("Error in communication with Data Node.");
			DcmisDB.close();
			throw new ProcessDeletionItemException(e);
		} catch (DataNodeClientException e) {
			m_logger.error("Error from Data Node.");
			DcmisDB.close();
			throw new ProcessDeletionItemException(e);
		} catch (Exception e) {
			m_logger.error("Error in update the post trend DB.");
			DcmisDB.close();
			throw new ProcessDeletionItemException(e);
		} finally {
			pt.close();
			DB.close();
		}
		
		try {
			m_perfLogger.logItemDeletePerformance(totalItemCount, startDT);
		} catch (IOException e) {
			m_logger.error("Error in logging the performance .");
		}
		
		try {
			DcmisDB.createConnection();
			Connection conn = DcmisDB.getConnection();
			Timestamp currentdate = new Timestamp(System.currentTimeMillis());
			String sql = "update T_DELETION set FINISH_TIME = ? ,STATUS = 2 where STATUS = 1 and TYPE = 1";
			PreparedStatement ps;
			try {
				ps = conn.prepareStatement(sql);
				ps.setTimestamp(1, currentdate);
				ps.execute();
				DcmisDB.close();
			} catch (SQLException e) {
				m_logger.error("Error in update info of DCMIS db.");
				throw new ProcessDeletionItemException(e);
			}
		} catch (Exception e1) {
			m_logger.error("Error in update info of DCMIS db.");
			DcmisDB.close();
			throw new ProcessDeletionItemException(e1);
		}
	}

	public static String generateQueryStr(String source, String siteid,
			String forumid, String threadid, int year, int month,
			Date startdate, Date enddate) {
		String queryStr = String.format("source:%s", source);

		if(siteid != null)
			queryStr = queryStr.concat(" AND siteid:"+siteid);
		if (year != 0)
			queryStr = queryStr.concat(" AND year:" + year);

		if (month != 0)
			queryStr = queryStr.concat(" AND month:" + month);

		if (forumid != null && forumid != "")
			queryStr = queryStr.concat(" AND forumid:" + forumid);

		if (threadid != null && threadid != "")
			queryStr = queryStr.concat(" AND threadid:" + threadid);

		if (startdate != null) {
			long starttime = startdate.getTime();
			long endtime = 0;
			String dateStr = null;
			if (enddate != null) {
				endtime = enddate.getTime();
				dateStr = String.format(" AND latestextractiondate:[%s TO %s]",
						starttime, endtime);
			} else {
				endtime = System.currentTimeMillis();
				dateStr = String.format(" AND latestextractiondate:[%s TO %s]",
						starttime, endtime);
			}
			queryStr = queryStr.concat(dateStr);
		}

		return queryStr;
	}
	
	private ArrayList<String> getForumID(Connection conn, int year, int month, String siteid) throws SQLException {
		ArrayList<String> forumidList = new ArrayList<String>();

		String sql = String.format("select * from T_POSTTREND where year = '%s' and month = '%s' and site_id = '%s' and post_count != 0", year, month, siteid);
		Statement stat = conn.createStatement();
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		while(result.next()) {
			String forumid = result.getString("forum_id");
			forumidList.add(forumid);
		}
		return forumidList;
	}
	
	private boolean isCleanPartition(String threadid, Date startDate) {
		if(threadid != null || startDate != null)
			return false;
		return true;
	}
}
class DeleteCondition {
	PartitionKey parkey = null;
	String condition = null;
	int count = 0;
	boolean isCleanPartition = true;

	public DeleteCondition(PartitionKey key, String con, boolean isClean) {
		this.setParkey(key);
		this.setCondition(con);
		this.setCleanPartition(isClean);
	}
	
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public PartitionKey getParkey() {
		return parkey;
	}
	public void setParkey(PartitionKey parkey) {
		this.parkey = parkey;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	public boolean isCleanPartition() {
		return isCleanPartition;
	}
	public void setCleanPartition(boolean isCleanPartition) {
		this.isCleanPartition = isCleanPartition;
	}
}
