package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.GetDNPartitionItemCountRequest;
import com.cic.textengine.repository.namenode.client.response.GetDNPartitionItemCountResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;

/**
 * This request need to be syncrhonized
 * @author denis.yu
 */
public class GetDNPartitionItemCountRequestHandler implements NNRequestHandler{
	Logger m_logger = Logger.getLogger(GetDNPartitionItemCountRequestHandler.class);
	
	public synchronized void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		GetDNPartitionItemCountRequest request = new GetDNPartitionItemCountRequest();
		request.read(is);
		
		GetDNPartitionItemCountResponse response = 
			new GetDNPartitionItemCountResponse();
		long item_count = 0;
		try {
			item_count = NameNodeManagerFactory.getNameNodeManagerInstance()
					.getDNPartitionItemCount(request.getDNKey(),
							request.getYear(), request.getMonth(),
							request.getSiteID(), request.getForumID());
		} catch (NameNodeManagerException e) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("Error retriving data:" + e.getMessage());
			response.write(os);
			m_logger.error("Exception:", e);
			return;
		}
		
		//validate the data name
		response.setItemCount(item_count);
		response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");
		
		response.write(os);
	}

}
