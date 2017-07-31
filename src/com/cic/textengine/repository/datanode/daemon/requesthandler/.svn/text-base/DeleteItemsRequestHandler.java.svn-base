package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.DeleteItemsRequest;
import com.cic.textengine.repository.datanode.client.response.AddItemsByStreamResponse;
import com.cic.textengine.repository.datanode.client.response.DeleteItemsResponse;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.partitionlock.IllegalPartitionLockStatusException;
import com.cic.textengine.repository.partitionlock.NoPartitionLockFoundException;
import com.cic.textengine.repository.partitionlock.PartitionAlreadyLockedException;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;

public class DeleteItemsRequestHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(DeleteItemsRequestHandler.class);

	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		DeleteItemsRequest request = new DeleteItemsRequest();
		request.read(is);

		//check if this data node is already registered on name node
		if (requestContext.getDaemon().getNNDaemonIP()==null){
			DeleteItemsResponse response = new DeleteItemsResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Data node unknown for name node.");
			response.write(os);
			return;
		}

		
		NameNodeClient nn_client = new NameNodeClient(requestContext
				.getDaemon().getNNDaemonIP(), requestContext.getDaemon()
				.getNNDaemonPort());
	
		//check local partition write lock on data node.
		PartitionWriteLockManager pwlm = null;
		pwlm = PartitionWriteLockManager.getInstance();
		try {
			pwlm.applyLock(request.getYear(), request.getMonth(), request
					.getSiteID(), request.getForumID(), requestContext
					.getSocket().getInetAddress().getHostAddress(),
					requestContext.getDaemon().getDataNodeKey(), 3);
		} catch (PartitionAlreadyLockedException e) {
			AddItemsByStreamResponse response = new AddItemsByStreamResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Can't apply the local parition write lock.");
			response.write(os);
			
			m_logger.error("Error", e);
			return;
		}		
		
		int targetVersion = -1;
		//apply partition write lock from name node
		try {
			targetVersion = nn_client.applyPartitionWriteLock4Delete(requestContext.getDaemon().getDataNodeKey(), 	//data node key
					requestContext.getSocket().getLocalAddress().toString(),	//ip address 
					request.getYear(),
					request.getMonth(), 
					request.getSiteID(), 
					request.getForumID(),
					request.listItemIDs(), request.isSorted());
		} catch (NameNodeClientException e1) {
			AddItemsByStreamResponse response = new AddItemsByStreamResponse();
			response.setErrorCode(DataNodeConst.ERROR_IDFENGINE);
			response.setErrorMsg("Can not apply for partition write lock because of:" + e1.getMessage());
			response.write(os);
			
			m_logger.debug("Can't apply for partition write lock.", e1);
			
			//release the local lock
			try {
				pwlm.releaseLock(request.getYear(), request.getMonth(), request
						.getSiteID(), request.getForumID(), requestContext
						.getSocket().getInetAddress().getHostAddress(),
						requestContext.getDaemon().getDataNodeKey(), 0);
			} catch (NoPartitionLockFoundException e) {
				//should not happen
			} 
			catch (IllegalPartitionLockStatusException e) {
				//should not happen
			}

			return;
		}		
		
		DeleteItemsResponse response = new DeleteItemsResponse();
		boolean succ = false;
		try {
			requestContext.getDaemon().getRepositoryEngine().deleteItems(
					request.getYear(), request.getMonth(), request.getSiteID(),
					request.getForumID(), request.listItemIDs(), request.isSorted());

			response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("");
			succ = true;
		} catch (RepositoryEngineException e) {
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg("Failed to delete items from partition:" + e.getMessage());
			m_logger.error("Exception", e);
			return;
		} finally{
			//release the local lock
			try {
				pwlm.releaseLock(request.getYear(), request.getMonth(), request
						.getSiteID(), request.getForumID(), requestContext
						.getSocket().getInetAddress().getHostAddress(),
						requestContext.getDaemon().getDataNodeKey(), succ?3:0);
			} catch (NoPartitionLockFoundException e1) {
			} catch (IllegalPartitionLockStatusException e1) {
			}
			
			//release name node lock (cancel the operation)
			this.releaseNNLock(nn_client, requestContext.getDaemon()
					.getDataNodeKey(), request.getYear(), request.getMonth(),
					request.getSiteID(), request.getForumID(),targetVersion, succ);
		}

		response.write(os);
	}

	void releaseNNLock(NameNodeClient nn_client, String key, int year,
			int month, String siteid, String forumid,
			int targetVersion, boolean succ) {
		try {
			int operation = 3;
			if (!succ)
				operation = 0;
			nn_client.releasePartitionWriteLock(key, 	//data node key
					year,
					month, 
					siteid, 
					forumid,
					operation,
					0,
					0,
					targetVersion
					);
		} catch (NameNodeClientException e) {
			m_logger.error("Failed to release write lock on name node.", e);
		}		
	}
}
