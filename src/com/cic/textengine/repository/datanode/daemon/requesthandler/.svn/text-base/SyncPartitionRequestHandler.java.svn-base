package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.SyncPartitionRequest;
import com.cic.textengine.repository.datanode.client.response.SyncPartitionResponse;

public class SyncPartitionRequestHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(SyncPartitionRequestHandler.class);

	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		SyncPartitionRequest request = new SyncPartitionRequest();
		request.read(is);

		SyncPartitionResponse response = new SyncPartitionResponse();

		requestContext.getDaemon().getDNSynchronizer().addSyncTask(
				request.getYear(), request.getMonth(), request.getSiteID(),
				request.getForumID(), request.getVersion());

		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");

		response.write(os);
	}

}
