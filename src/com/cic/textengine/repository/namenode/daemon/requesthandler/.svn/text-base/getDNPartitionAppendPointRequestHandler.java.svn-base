package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.GetDNPartitionAppendPointRequest;
import com.cic.textengine.repository.namenode.client.response.GetDNPartitionAppendPointResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;

public class getDNPartitionAppendPointRequestHandler implements NNRequestHandler{

	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		GetDNPartitionAppendPointRequest request = new GetDNPartitionAppendPointRequest();
		request.read(is);
		
		//validate the data name
		GetDNPartitionAppendPointResponse response = 
			new GetDNPartitionAppendPointResponse();
		try {
			
			long startItemID = -1;
			startItemID = NameNodeManagerFactory.getNameNodeManagerInstance()
					.getDNPartitionAppendPoint(request.getDNKey(),
							request.getYear(), request.getMonth(),
							request.getSiteID(), request.getForumID());
			
			response.setStartItemID(startItemID);	
			response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("success");
		} catch (NameNodeManagerException e1) {
			
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg(e1.getMessage());
		}
		
		
		response.write(os);
	}

}
