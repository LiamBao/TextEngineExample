package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.UpdateDNPartitionVersionRequest;
import com.cic.textengine.repository.namenode.client.response.UpdateDNPartitionVersionResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;

public class UpdateDNPartitionVersionRequestHandler implements NNRequestHandler{
	Logger m_logger = Logger.getLogger(UpdateDNPartitionVersionRequestHandler.class);
	
	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {

		UpdateDNPartitionVersionRequest request = new UpdateDNPartitionVersionRequest();
		request.read(is);
		
		
		UpdateDNPartitionVersionResponse response = new UpdateDNPartitionVersionResponse();
		response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
		
		try {
			NameNodeManagerFactory.getNameNodeManagerInstance()
					.updateDNPartitionVersion(request.getDNKey(),
							request.getYear(), request.getMonth(),
							request.getSiteID(), request.getForumID(),
							request.getItemCount(),
							request.getVersion());
		} catch (NameNodeManagerException e) {
			m_logger.error("Exception:", e);
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg(e.getMessage());
		}
		
		response.write(os);
		
	}
}
