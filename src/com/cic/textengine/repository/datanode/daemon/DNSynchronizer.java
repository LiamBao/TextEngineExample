package com.cic.textengine.repository.datanode.daemon;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.namenode.client.type.PartitionOperation;

public class DNSynchronizer implements Runnable{
	Logger m_logger = Logger.getLogger(DNSynchronizer.class);
	
	boolean m_stop = false;
	
	ArrayBlockingQueue<SyncTask> m_syncTasks = new ArrayBlockingQueue<SyncTask>(5000, true);
	
	DNDaemon m_dndaemon = null;
	
	Thread m_thread = null;

	DNSynchronizer(DNDaemon dndaemon){
		m_dndaemon = dndaemon;
	}

	public void start(){
		m_stop = false;
		m_thread = new Thread(this);
		m_thread.start();
	}
	
	public void run() {
		SyncTask st = null;
		while(!m_stop){
			try {
				st = m_syncTasks.take();
				m_logger.debug("Sync task retrieved:" + st.toString());
				synchronizeDNPartition(st);
				
			} catch (InterruptedException e) {
				m_logger.error("Exception", e);
			}
		}
	}
	
	void synchronizeDNPartition(SyncTask st){
		NameNodeClient nn_client = new NameNodeClient(m_dndaemon
				.getNNDaemonIP(), m_dndaemon.getNNDaemonPort());
		PartitionOperation po = null;
		
		
		try {
			po = nn_client.getNextDNPartitionOperation(m_dndaemon.getDataNodeKey(), st
					.getYear(), st.getMonth(), st.getSiteid(), st.getForumid());
		} catch (NameNodeClientException e) {
			m_logger.error("Exception:", e);
			return;
		} catch (DataNodeClientCommunicationException e) {
			m_logger.error("Exception:", e);
			return;
		}
		
		long itemCount = -1;
		
		while (po != null && po.getVersion() <= st.getVersion() && po.getVersion() > 0){
			itemCount = -1;
			switch(po.getOperation()){
			case 1://partition add
				itemCount = 
					syncPartitionAppend(st, po);
				break;
			case 3://partition delete
				itemCount = syncPartitionDelete(st, po);
				break;
			}

			if (itemCount >= 0){
				try {
					nn_client.updateDNPartitionVersion(m_dndaemon.getDataNodeKey(), st
							.getYear(), st.getMonth(), st.getSiteid(), st.getForumid(),
							itemCount,
							po.getVersion());
				} catch (NameNodeClientException e) {
					m_logger.error("Exception:", e);
					return;
				} catch (DataNodeClientCommunicationException e) {
					m_logger.error("Exception:", e);
					return;
				}
			}

			//retrieve next partition operation.
			try {
				po = nn_client.getNextDNPartitionOperation(m_dndaemon.getDataNodeKey(), st
						.getYear(), st.getMonth(), st.getSiteid(), st.getForumid());
			} catch (NameNodeClientException e) {
				m_logger.error("Exception:", e);
				return;
			} catch (DataNodeClientCommunicationException e) {
				m_logger.error("Exception:", e);
				return;
			}
		} 
	}
	
	long syncPartitionAppend(SyncTask st, PartitionOperation po){
		DataNodeClient dn_client = new DataNodeClient(po.getSeedDNHost(), po
				.getSeedDNPort());

		PartitionWriter pw = null;
		RemoteTEItemEnumerator ie = null;
		try {
			ie = dn_client.getItemEnumerator(st.getYear(), st.getMonth(), st
					.getSiteid(), st.getForumid(), po.getStartItemID(), po.getItemCount(), true);
			pw = m_dndaemon.getRepositoryEngine().getPartitionWriter(st.getYear(), st
					.getMonth(), st.getSiteid(), st.getForumid(), po.getStartItemID());
			long count = 0;
			while(ie.next() && count < po.getItemCount()){
				pw.writeItem(ie.getItem());
				count++;
			}
			pw.flush();
			if (po.getItemCount() != count){
				m_logger.fatal("Partition item count is not synchronized [Y:" + po.getYear() + ",M:" + po.getMonth() +
						",S:" + po.getSiteID() + ",F:" + po.getForumID()+"]"+"OperationItemCount:"+po.getItemCount()+". PartitionItemCount:"+count);
				return -1;
			}
			return po.getStartItemID() + count - 1;
		} catch (DataNodeClientException e) {
			m_logger.error("Exception:",e);
			return -1;
		} catch (DataNodeClientCommunicationException e) {
			m_logger.error("Exception:",e);
			return -1;
		} catch (RepositoryEngineException e) {
			m_logger.error("Exception:",e);
			return -1;
		} catch (IOException e) {
			m_logger.error("Exception:",e);
			return -1;
		} finally{
			if (ie != null)
				ie.close();
			try {
				if (pw != null)
					pw.close();
			} catch (RepositoryEngineException e) {}
		}
	}
	
	long syncPartitionDelete(SyncTask st, PartitionOperation po){
		try {
			m_dndaemon.getRepositoryEngine().deleteItems(
					st.getYear(),st.getMonth(), st.getSiteid(), st.getForumid(), 
					po.listDeletedItemIDList(), po.isDeleteItemIDListSorted());
			//return zero so the nn won't update the actual partition item count.
			return 0;
		} catch (RepositoryEngineException e) {
			m_logger.error("Exception:",e);
			return -1;
		}		
	}
	
	public void addSyncTask(int year, int month, String siteid,
			String forumid, int version) {
		SyncTask t = new SyncTask(year, month, siteid, forumid, version);
		m_logger.debug("New Sync Task Offered:" + t.toString() + ",Remaining Capacity in Queue:" + m_syncTasks.remainingCapacity());
		m_syncTasks.offer(t);
	}
	
	class SyncTask{
		int year, month, version;
		String siteid, forumid;
		
		SyncTask(int year, int month, String siteid, String forumid, int version){
			this.setYear(year);
			this.setMonth(month);
			this.setSiteid(siteid);
			this.setForumid(forumid);
			this.setVersion(version);
		}

		public int getYear() {
			return year;
		}

		public void setYear(int year) {
			this.year = year;
		}

		public int getMonth() {
			return month;
		}

		public void setMonth(int month) {
			this.month = month;
		}

		public String getSiteid() {
			return siteid;
		}

		public void setSiteid(String siteid) {
			this.siteid = siteid;
		}

		public String getForumid() {
			return forumid;
		}

		public void setForumid(String forumid) {
			this.forumid = forumid;
		}

		public int getVersion() {
			return version;
		}

		public void setVersion(int version) {
			this.version = version;
		}
		
		public String toString(){
			return "SyncTask[Y:" + this.getYear() +
			",M:" + this.getMonth() + ",S:" + this.getSiteid()
			+ ",F:" + this.getForumid() + ",V:" + this.getVersion() + "]";
		}
	}

}
