package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.ReleasePartitionWriteLockRequest;
import com.cic.textengine.repository.namenode.client.response.ReleasePartitionWriteLockResponse;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistry;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.type.DataNode;
import com.cic.textengine.repository.namenode.strategy.DNChooseStrategyFactory;
import com.cic.textengine.repository.partitionlock.IllegalPartitionLockStatusException;
import com.cic.textengine.repository.partitionlock.NoPartitionLockFoundException;
import com.cic.textengine.repository.partitionlock.PartitionWriteLock;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;

public class ReleasePartitionWriteLockRequestHandler implements
		NNRequestHandler {
	Logger m_logger = Logger
			.getLogger(ReleasePartitionWriteLockRequestHandler.class);

	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {

		ReleasePartitionWriteLockRequest request = new ReleasePartitionWriteLockRequest();
		request.read(is);

		PartitionWriteLockManager pwlm = null;
		pwlm = PartitionWriteLockManager.getInstance();
		PartitionWriteLock lock = pwlm.getLock(request.getYear(), request
				.getMonth(), request.getSiteID(), request.getForumID());

		ReleasePartitionWriteLockResponse response = new ReleasePartitionWriteLockResponse();

		if (lock == null) {
			response.setErrorCode(NameNodeConst.ERROR_PARTITION_LOCK_NOT_FOUND);
			response
					.setErrorMsg("Partition is not locked. Can not release lock.");
			response.write(os);
			m_logger.debug("Partition is not locked. Can not release lock.");
			return;
		}

		if (!((lock.getDataNodeKey().equals(request.getDNKey())) && (lock
				.getIP().equals(requestContext.getSocket().getInetAddress()
				.getHostAddress())))) {
			response.setErrorCode(NameNodeConst.ERROR_PARTITION_LOCK_NOT_MATCH);
			response
					.setErrorMsg("Data node ["
							+ request.getDNKey()
							+ "] does not own the partition write lock. Can not release the lock.");
			response.write(os);
			m_logger
					.debug("Data node ["
							+ request.getDNKey()
							+ "] does not own the partition write lock. Can not release the lock.");
			return;
		}

		if (lock.getOperation() != request.getOperation()
				&& request.getOperation() != 0) {
			response.setErrorCode(NameNodeConst.ERROR_PARTITION_LOCK_NOT_MATCH);
			response
					.setErrorMsg("The partition is locked for a different operation as the lock asked to be release. Release lock is failed.");
			response.write(os);
			m_logger
					.debug("The partition is locked for a different operation as the lock asked to be release. Release lock is failed.");
			return;
		}

		response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("success");

		int new_partition_version = 0;
		switch (request.getOperation()) {
		case 1:// append partition
			if (request.getItemCount() > 0) {
				try {
					ArrayList<String> dnkeylist = new ArrayList<String>();
					dnkeylist.add(request.getDNKey());
					new_partition_version = NameNodeManagerFactory
							.getNameNodeManagerInstance().finishPartitionWrite(
									request.getYear(), request.getMonth(),
									request.getSiteID(), request.getForumID(),
									request.getStartItemID(),
									request.getItemCount(), dnkeylist);
				} catch (NameNodeManagerException e) {
					response.setErrorCode(NameNodeConst.ERROR_GENERAL);
					response.setErrorMsg(e.getMessage());
					m_logger.error("Exception:", e);
				}

			}
			break;
		case 2:// clean partition
			try {
				new_partition_version = NameNodeManagerFactory
						.getNameNodeManagerInstance().cleanPartition(
								request.getYear(), request.getMonth(),
								request.getSiteID(), request.getForumID());
			} catch (NameNodeManagerException e) {
				response.setErrorCode(NameNodeConst.ERROR_GENERAL);
				response.setErrorMsg(e.getMessage());
				m_logger.error("Exception:", e);
			}
			break;
		case 3:// delete partition
			ArrayList<String> dnkeylist = new ArrayList<String>();
			dnkeylist.add(request.getDNKey());
			try {
				new_partition_version = NameNodeManagerFactory.getNameNodeManagerInstance()
						.finishPartitionDelete(request.getYear(),
								request.getMonth(), request.getSiteID(),
								request.getForumID(),
								request.getTargetVersion(), dnkeylist);
			} catch (NameNodeManagerException e) {
				response.setErrorCode(NameNodeConst.ERROR_GENERAL);
				response.setErrorMsg(e.getMessage());
				m_logger.error("Exception:", e);
			}
			break;
		}

		if (new_partition_version > 0){
			// send the partition replication task to all data nodes
			ArrayList<DataNode> replica_dn_list = DNChooseStrategyFactory
					.getDNChooseStrategyInstance()
					.chooseDNForPartitionReplication(request.getYear(),
							request.getMonth(), request.getSiteID(),
							request.getForumID());
			for (DataNode dn : replica_dn_list) {
				if (!dn.getKey().equals(request.getDNKey())) {
					DataNodeClient dn_client = new DataNodeClient(dn
							.getIP(), dn.getPort());
					try {
						dn_client.syncPartition(request.getYear(), request
								.getMonth(), request.getSiteID(), request
								.getForumID(), new_partition_version);
					} catch (DataNodeClientCommunicationException e) {
						m_logger.error("Exception", e);
					} catch (DataNodeClientException e) {
						m_logger.error("Exception", e);
					}
				}
			}
		}
		
		// update the dnregistry table in mem.
		DNRegistry dnreg = DNRegistryTable.getInstance().getDNRegistry(
				request.getDNKey());
		if (dnreg != null) {
			dnreg.resetTTL();
		}

		try {
			pwlm.releaseLock(request.getYear(), request.getMonth(), request
					.getSiteID(), request.getForumID(), requestContext
					.getSocket().getInetAddress().getHostAddress(), request
					.getDNKey(), request.getOperation());
		} catch (NoPartitionLockFoundException e) {
			response.setErrorCode(NameNodeConst.ERROR_PARTITION_LOCK_NOT_FOUND);
			response.setErrorMsg("Partition is not locked.");
			m_logger.error("Partition is not locked.",e);
		} catch (IllegalPartitionLockStatusException e) {
			response.setErrorCode(NameNodeConst.ERROR_PARTITION_LOCK_NOT_MATCH);
			response
					.setErrorMsg("Data node ["
							+ request.getDNKey()
							+ "] does not own the partition write lock. Can not release the lock.");
			m_logger
					.error("Data node ["
							+ request.getDNKey()
							+ "] does not own the partition write lock. Can not release the lock.");
		}

		response.write(os);
	}
}
