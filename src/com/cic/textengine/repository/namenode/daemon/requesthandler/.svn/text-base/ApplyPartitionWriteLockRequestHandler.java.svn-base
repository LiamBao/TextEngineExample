package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.ApplyPartitionWriteLockRequest;
import com.cic.textengine.repository.namenode.client.response.ApplyPartitionWriteLockResponse;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistry;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.partitionlock.IllegalPartitionLockStatusException;
import com.cic.textengine.repository.partitionlock.NoPartitionLockFoundException;
import com.cic.textengine.repository.partitionlock.PartitionAlreadyLockedException;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;

/**
 * This request need to be syncrhonized
 * 
 * @author denis.yu
 */
public class ApplyPartitionWriteLockRequestHandler implements NNRequestHandler {
	Logger m_logger = Logger
			.getLogger(ApplyPartitionWriteLockRequestHandler.class);

	public synchronized void handleRequest(NNRequestContext requestContext,
			InputStream is, OutputStream os) throws IOException {
		ApplyPartitionWriteLockRequest request = new ApplyPartitionWriteLockRequest();
		request.read(is);

		//check if the data node is an active data node.
		DNRegistry dnreg = DNRegistryTable.getInstance().getDNRegistry(
				request.getDNKey());
		ApplyPartitionWriteLockResponse response = new ApplyPartitionWriteLockResponse();
		response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");
		
		if (dnreg == null) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("The data node is not known as active. ");
			response.write(os);
			return;
		}
		// update the dnregistry table in mem.
		dnreg.resetTTL();

		
		PartitionWriteLockManager pwlm = null;
		pwlm = PartitionWriteLockManager.getInstance();

		try {
			pwlm.applyLock(request.getYear(), request.getMonth(), request
					.getSiteID(), request.getForumID(), requestContext
					.getSocket().getInetAddress().getHostAddress(), request
					.getDNKey(), request.getOperation());
			response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("success");
		} catch (PartitionAlreadyLockedException e) {
			// can not apply partition lock, exit the process.
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg(e.getMessage());
			response.write(os);
			m_logger.error("Exception:", e);
			return;
		}
		

		//check if the data node has the latest version of partition
		if (request.getOperation() == 1 || request.getOperation() == 3){
			try {
				if (!NameNodeManagerFactory.getNameNodeManagerInstance()
						.isDNPartitionVersionLatest(request.getYear(),
								request.getMonth(), request.getSiteID(),
								request.getForumID(), request.getDNKey())){
					response.setErrorCode(NameNodeConst.ERROR_GENERAL);
					response
							.setErrorMsg("Datanode does not contains the latest version of partitoin, can not be used to write.");
					m_logger.error("Datanode does not contains the latest version of partitoin, can not be used to write.");
				}
			} catch (NameNodeManagerException e) {
				response.setErrorCode(NameNodeConst.ERROR_GENERAL);
				response.setErrorMsg(e.getMessage());
				m_logger.error("Exception:", e);
			}
			
		}
		
		if (response.getErrorCode() == NameNodeConst.ERROR_SUCCESS){
		
			switch (request.getOperation()) {
			case 1:// append
				try {
					long startItemID = -1;
					startItemID = NameNodeManagerFactory
							.getNameNodeManagerInstance()
							.getDNPartitionAppendPoint(request.getDNKey(),
									request.getYear(), request.getMonth(),
									request.getSiteID(), request.getForumID());
					if (startItemID < 1) {
						response.setErrorCode(NameNodeConst.ERROR_GENERAL);
						response
								.setErrorMsg("Can not use this data node for appending data to the parition");
						m_logger
								.error("Can not use this data node for appending data to the parition.");
					}
					response.setStartItemID(startItemID);
				} catch (NameNodeManagerException e) {
					response.setErrorCode(NameNodeConst.ERROR_GENERAL);
					response
							.setErrorMsg("Can not use this data node for appending data to the parition: "
									+ e.getMessage());
					m_logger
							.error(
									"Can not use this data node for appending data to the parition: ",
									e);
				}
	
				break;
			case 2:// clean, clean the name node registration
				try {
					NameNodeManagerFactory.getNameNodeManagerInstance()
							.cleanPartition(request.getYear(), request.getMonth(),
									request.getSiteID(), request.getForumID());
				} catch (NameNodeManagerException e) {
					try {
						pwlm.releaseLock(request.getYear(), request.getMonth(),
								request.getSiteID(), request.getForumID(),
								requestContext.getSocket().getInetAddress()
										.getHostAddress(), request.getDNKey(),
								request.getOperation());
					} catch (NoPartitionLockFoundException e1) {
					}// the following should not happen here
					catch (IllegalPartitionLockStatusException e1) {
					}
	
					response.setErrorCode(NameNodeConst.ERROR_GENERAL);
					response.setErrorMsg(e.getMessage());
					m_logger.error("Exception:", e);
				}
				break;
			case 3:
				try {
					int targetVersion = -1;
					targetVersion = NameNodeManagerFactory
							.getNameNodeManagerInstance()
							.logPartitionDeleteOperation(request.getYear(),
									request.getMonth(), request.getSiteID(),
									request.getForumID(), request.getItemIDList(),
									request.isSorted());
					response.setTargetVersion(targetVersion);
				} catch (NameNodeManagerException e) {
					try {
						pwlm.releaseLock(request.getYear(), request.getMonth(),
								request.getSiteID(), request.getForumID(),
								requestContext.getSocket().getInetAddress()
										.getHostAddress(), request.getDNKey(),
								request.getOperation());
					} catch (NoPartitionLockFoundException e1) {
					}// the following should not happen here
					catch (IllegalPartitionLockStatusException e1) {
					}
	
					response.setErrorCode(NameNodeConst.ERROR_GENERAL);
					response.setErrorMsg(e.getMessage());
					m_logger.error("Exception:", e);
				}
				break;
			}
		}
		
		if (response.getErrorCode() != NameNodeConst.ERROR_SUCCESS){
			//release the partitiom lock if fail
			try {
				pwlm.releaseLock(request.getYear(), request.getMonth(), request
							.getSiteID(), request.getForumID(), requestContext
							.getSocket().getInetAddress().getHostAddress(), request
							.getDNKey(), 0);
			} catch (NoPartitionLockFoundException e2) {
				//ignore
			} catch (IllegalPartitionLockStatusException e2) {
				//ignore
			}
		}
		response.write(os);
	}
}
