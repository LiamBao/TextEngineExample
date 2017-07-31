package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.AssignNNDaemonRequest;
import com.cic.textengine.repository.datanode.client.response.AssignNNDaemonResponse;

public class AssignNNDaemonRequestHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(AssignNNDaemonRequestHandler.class);
	
	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		AssignNNDaemonRequest request = new AssignNNDaemonRequest();
		request.read(is);
		
		request.setNNDaemonIP(requestContext.getSocket().getInetAddress().getHostAddress());
		
		AssignNNDaemonResponse response = new AssignNNDaemonResponse();
		if (requestContext.getDaemon().getNNDaemonIP() == null){
			requestContext.getDaemon().setNNDaemonIP(request.getNNDaemonIP());
			requestContext.getDaemon().setNNDaemonPort(request.getNNDaemonPort());
			response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("");
			
			m_logger.info("Name node daemon assigned [ip:" + request.getNNDaemonIP() + ",port:" + request.getNNDaemonPort() + "]");
		}else{
			if (request.getNNDaemonIP().equalsIgnoreCase(requestContext.getDaemon().getNNDaemonIP()) &&
					request.getNNDaemonPort() == requestContext.getDaemon().getNNDaemonPort()){
				//name node registry is same here, return succ message.
				response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
				response.setErrorMsg("");
			}else{
				response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
				response.setErrorMsg("Different name node registry on this data node, refuse registing new name node.");
				m_logger.error("Name node already exists, refuse new name node assignment. " +
						"\n   Existing NN [ip:" + requestContext.getDaemon().getNNDaemonIP() + ",port:" + requestContext.getDaemon().getNNDaemonPort() + "]" + 
						"\n   New NN [ip:" + request.getNNDaemonIP() + ",port:" + request.getNNDaemonPort() + "]");
			}
		}
		response.write(os);
	}

}
