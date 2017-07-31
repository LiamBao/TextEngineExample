package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.GetDNClientForWritingRequest;
import com.cic.textengine.repository.namenode.client.response.GetDNClientForWritingResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.exception.NoDataNodeAvaliableForPartitionWrite;
import com.cic.textengine.repository.namenode.manager.type.DataNode;
import com.cic.textengine.repository.namenode.strategy.DNChooseStrategyFactory;

/**
 * This request need to be syncrhonized
 * @author denis.yu
 */
public class GetDNClientForAppendRequestHandler implements NNRequestHandler{
	Logger m_logger = Logger.getLogger(GetDNClientForAppendRequestHandler.class);
	
	public synchronized void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		GetDNClientForWritingRequest request = new GetDNClientForWritingRequest();
		request.read(is);
		
		GetDNClientForWritingResponse response = 
			new GetDNClientForWritingResponse();
		ArrayList<String> dnkey_list = null;
		DataNode dn = null;
		try {
			dnkey_list = NameNodeManagerFactory.getNameNodeManagerInstance()
					.getDataNodeForPartitionWrite(request.getYear(),
							request.getMonth(), request.getSiteID(),
							request.getForumID());
			
		} catch (NameNodeManagerException e) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("Can not get a data node for append data:" + e.getMessage());
			response.write(os);
			m_logger.error("Can not get a data node for append data.", e);
			return;
		} catch (NoDataNodeAvaliableForPartitionWrite e) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("Can not get a data node for append data:" + e.getMessage());
			response.write(os);
			m_logger.error("Can not get a data node for append data.", e);
			return;
		}
		
		dn = DNChooseStrategyFactory.getDNChooseStrategyInstance().chooseDNForPartitionWrite(request.getYear(),
							request.getMonth(), request.getSiteID(),
							request.getForumID(), dnkey_list);
		if (dn == null){
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("Can not get a data node for append data, all qualified data nodes are not active.");
			response.write(os);
			m_logger.error("Can not get a data node for append data, all qualified data nodes are not active.");
			return;
		}
		
		//validate the data name

		response.setHost(dn.getIP());
		response.setPort(dn.getPort());
		response.setDNKey(dn.getKey());
		response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");
		
		response.write(os);
	}

}
