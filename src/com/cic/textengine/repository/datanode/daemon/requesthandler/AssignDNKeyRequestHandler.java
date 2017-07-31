package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.AssignKeyRequest;
import com.cic.textengine.repository.datanode.client.response.AssignKeyResponse;
import com.cic.textengine.repository.datanode.daemon.exception.DNDaemonException;
import com.cic.textengine.repository.datanode.daemon.exception.DataNodeKeyAlreadyExistsException;

public class AssignDNKeyRequestHandler implements DNRequestHandler {
	
	Logger m_logger = Logger.getLogger(AssignDNKeyRequestHandler.class);

	public void handleRequest(DNRequestContext requestContext, InputStream is, OutputStream os)
			throws IOException {
		AssignKeyRequest request = new AssignKeyRequest();
		request.read(is);
		
		m_logger.debug("Assign key for this data node");
		m_logger.debug("  key:" + request.getKey());
		
		AssignKeyResponse response = new AssignKeyResponse();
		if (requestContext.getDaemon().getDataNodeKey() != null){
			m_logger.error("The data node key already exists, can't not assign new key to this data node.");
			
			response.setErrorCode(DataNodeConst.ERROR_DN_KEY_EXISTS);
			response.setErrorMsg("This data node already has a key assigned, no need to assign another key.");
		}else{
			try {

				requestContext.getDaemon().setDataNodeKey(request.getKey());
				response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
				response.setErrorMsg("success");
				
				m_logger.debug("Data node key is assigned and saved.");
			} catch (DNDaemonException e) {
				response.setErrorCode(DataNodeConst.ERROR_DN_SET_KEY);
				response.setErrorMsg("Failed to set data node key.");
				m_logger.error("Excpetion when set data node key:", e);
			} catch (DataNodeKeyAlreadyExistsException e) {
				response.setErrorCode(DataNodeConst.ERROR_DN_SET_KEY);
				response.setErrorMsg("Failed to set data node key.");
				m_logger.error("Excpetion when set data node key:", e);
			}
		}
		
		response.write(os);
	}
}
