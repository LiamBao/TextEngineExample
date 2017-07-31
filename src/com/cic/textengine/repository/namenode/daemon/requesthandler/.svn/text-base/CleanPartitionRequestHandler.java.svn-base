package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.CleanPartitionRequest;
import com.cic.textengine.repository.namenode.client.response.CleanPartitionResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.partitionlock.IllegalPartitionLockStatusException;
import com.cic.textengine.repository.partitionlock.NoPartitionLockFoundException;
import com.cic.textengine.repository.partitionlock.PartitionAlreadyLockedException;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;

/**
 * This request need to be syncrhonized
 * @author denis.yu
 */
public class CleanPartitionRequestHandler implements NNRequestHandler{
	Logger m_logger = Logger.getLogger(CleanPartitionRequestHandler.class);
	
	public synchronized void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		CleanPartitionRequest request = new CleanPartitionRequest();
		request.read(is);
		
		CleanPartitionResponse response = 
			new CleanPartitionResponse();

		//apply the write lock first.
		PartitionWriteLockManager pwlm = null;
		pwlm = PartitionWriteLockManager.getInstance();

		try {
			pwlm.applyLock(request.getYear(), request.getMonth(), request
					.getSiteID(), request.getForumID(), "", "", 2);
		} catch (PartitionAlreadyLockedException e) {
			// can not apply partition lock, exit the process.
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg(e.getMessage());
			response.write(os);
			m_logger.error("Exception:", e);
			return;
		}

		
		try {
			NameNodeManagerFactory.getNameNodeManagerInstance().cleanPartition(
					request.getYear(), request.getMonth(), request.getSiteID(),
					request.getForumID());

			//validate the data name
			response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("");
		} catch (NameNodeManagerException e) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("Error retriving data:" + e.getMessage());
			m_logger.error("Exception:", e);
		}
		
		response.write(os);
		
		try {
			pwlm.releaseLock(request.getYear(), request.getMonth(), request
					.getSiteID(), request.getForumID(), "", "", response
					.getErrorCode() == NameNodeConst.ERROR_SUCCESS ? 2 : 0);
		} catch (NoPartitionLockFoundException e) {
			//ignore
		} catch (IllegalPartitionLockStatusException e) {
			//ignore
		}
		
	}

}
