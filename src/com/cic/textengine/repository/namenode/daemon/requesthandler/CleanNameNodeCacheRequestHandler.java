package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.CleanNameNodeCacheRequest;
import com.cic.textengine.repository.namenode.client.response.CleanNameNodeCacheResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;

public class CleanNameNodeCacheRequestHandler implements NNRequestHandler{
	
	Logger m_logger = Logger.getLogger(CleanNameNodeCacheRequestHandler.class);
	
	@Override
	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		CleanNameNodeCacheRequest request = new CleanNameNodeCacheRequest();
		request.read(is);
		
		CleanNameNodeCacheResponse response = new CleanNameNodeCacheResponse();
		
		try {
			NameNodeManagerFactory.getNameNodeManagerInstance().cleanNameNodeCache();
			response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("");
		} catch (NameNodeManagerException e) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("Error clean name node cache:" + e.getMessage());
			m_logger.error("Exception:", e);
		}
		
		response.write(os);
		
	}

}
