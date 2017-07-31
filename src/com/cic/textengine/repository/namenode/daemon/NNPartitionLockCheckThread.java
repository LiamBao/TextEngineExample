package com.cic.textengine.repository.namenode.daemon;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistry;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;
import com.cic.textengine.repository.partitionlock.IllegalPartitionLockStatusException;
import com.cic.textengine.repository.partitionlock.NoPartitionLockFoundException;
import com.cic.textengine.repository.partitionlock.PartitionWriteLock;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;

/**
 * This thread checks the unreleased partition write lock on name node.
 * And try to release the lock if the corresponding data node has been
 * reset or can not be contacted.
 * 
 * @author denis.yu
 *
 */
public class NNPartitionLockCheckThread implements Runnable{
	Logger m_logger = Logger.getLogger(NNPartitionLockCheckThread.class);
	Thread m_thread = null;
	boolean m_stop = false;
	NNDaemon m_daemon = null;
	
	NNPartitionLockCheckThread(NNDaemon daemon){
		this.m_daemon = daemon;
	}
	
	public void start(){
		m_stop = false;
		m_thread = new Thread(this);
		m_thread.start();
	}
	
	public void run() {
		DNRegistry dnregistry = null;
		PartitionWriteLockManager pwlm = PartitionWriteLockManager.getInstance();
		int retry_times = 3;
		while(!m_stop){
			ArrayList<PartitionWriteLock> locks = pwlm.listExistingLock();
			
			PartitionWriteLock lock = null;
			for (int i = 0;i<locks.size();i++){

				lock = locks.get(i);
				
				if ("".equals(lock.getDataNodeKey())){
					m_logger.debug("Lock is not associated to any Datanode, release it immediately.");
					this.releaseLock(pwlm, lock);
					continue;
				}
				//check if the DN is still active
				dnregistry = DNRegistryTable.getInstance().getDNRegistry(lock.getDataNodeKey());

				if (dnregistry==null){//if the data node is not active, release the lock.
					m_logger.debug("The data node for the partition lock is not active, release the lock.");
					this.releaseLock(pwlm, lock);
					continue;
				}
				
				DataNodeClient dn_client = new DataNodeClient(dnregistry
						.getHost(), dnregistry.getPort());
				try {
					if (dn_client.checkDNPartitionWriteLock(lock.getYear(), lock
							.getMonth(), lock.getSiteID(), lock.getForumID()) != lock.getOperation()) {
						m_logger
								.debug("The partiton lock on the data node is already " +
										"released, or not valid any more. " +
										"Releae the lock on the name node.");
						this.releaseLock(pwlm, lock);
						retry_times = 3;
						continue;
					}
				}catch (DataNodeClientCommunicationException e) {
					
					m_logger
							.error("Failed to talk to data node to check the partitin lock [dn_key:"
									+ dnregistry.getDNKey()
									+ ",ip:"
									+ dnregistry.getHost()
									+ ",port:"
									+ dnregistry.getPort() + "]");
					//if can not talk to the DN, give 3 times retry. 
					//If still can not communicate to the DN, release the partition lock. 
					//The write operation on the data node will be failed when the DN tries
					//to release the write lock.
					if (retry_times<0){
						this.releaseLock(pwlm, lock);
						retry_times = 3;
						//deactive the data node in the dn registry table
						DNRegistryTable.getInstance().unregisterDN(dnregistry.getDNKey());
					}else{
						retry_times--;
						i--;
					}
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {}
				}
			}
			
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
			}
		}
	}

	void releaseLock(PartitionWriteLockManager pwlm, PartitionWriteLock lock){
		try {
			pwlm.releaseLock(lock.getYear(), lock.getMonth(), lock
					.getSiteID(), lock.getForumID(), lock.getIP(), lock
					.getDataNodeKey(), 0);
			m_logger
					.info("Release partition lock since it's not valid on the data node:"
							+ lock.toString());
		} catch (NoPartitionLockFoundException e) {
			//ignore
		} catch (IllegalPartitionLockStatusException e) {
			//ignore
		}
		
	}
}
