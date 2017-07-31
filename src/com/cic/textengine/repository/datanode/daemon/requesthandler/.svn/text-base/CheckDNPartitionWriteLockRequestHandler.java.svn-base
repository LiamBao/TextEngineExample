package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.CheckDNPartitionWriteLockRequest;
import com.cic.textengine.repository.datanode.client.response.CheckDNPartitionWriteLockResponse;
import com.cic.textengine.repository.partitionlock.PartitionWriteLock;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;

public class CheckDNPartitionWriteLockRequestHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(CheckDNPartitionWriteLockRequestHandler.class);

	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		CheckDNPartitionWriteLockRequest request = new CheckDNPartitionWriteLockRequest();
		request.read(is);

		CheckDNPartitionWriteLockResponse response = new CheckDNPartitionWriteLockResponse();

		PartitionWriteLockManager pwlm = null;
		pwlm = PartitionWriteLockManager.getInstance();
		PartitionWriteLock lock = pwlm.getLock(request.getYear(), request.getMonth(), request
				.getSiteID(), request.getForumID());

		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");
		if (lock == null){
			response.setOperation(0);
		}else{
			response.setOperation(lock.getOperation());
		}
		response.write(os);
	}

}
