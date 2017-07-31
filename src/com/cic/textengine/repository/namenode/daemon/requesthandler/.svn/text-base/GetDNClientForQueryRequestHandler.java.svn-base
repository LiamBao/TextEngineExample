package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.GetDNClientForQueryRequest;
import com.cic.textengine.repository.namenode.client.response.GetDNClientForQueryResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.type.DataNode;
import com.cic.textengine.repository.namenode.strategy.DNChooseStrategyFactory;

/**
 * This request need to be syncrhonized
 * @author denis.yu
 */
public class GetDNClientForQueryRequestHandler implements NNRequestHandler{
	Logger m_logger = Logger.getLogger(GetDNClientForQueryRequestHandler.class);
	
	public synchronized void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		GetDNClientForQueryRequest request = new GetDNClientForQueryRequest();
		request.read(is);
		
		GetDNClientForQueryResponse response = 
			new GetDNClientForQueryResponse();
		
		
		
		try {
			ArrayList<String> dnkey_list = NameNodeManagerFactory
					.getNameNodeManagerInstance().listPartitionDNForQuery(
							request.getYear(), request.getMonth(),
							request.getSiteID(), request.getForumID());

			DataNode dn = DNChooseStrategyFactory.getDNChooseStrategyInstance()
					.chooseDNForQuery(dnkey_list);
			
			if(dn == null){
				response.setErrorCode(NameNodeConst.ERROR_GENERAL);
				response.setErrorMsg("No active data node can be found for querying data from parititon.");
				response.write(os);
				m_logger.error("No active data node can be found for querying data from parititon.");
				return;
			}else{
				response.setHost(dn.getIP());
				response.setPort(dn.getPort());
				response.setDNKey(dn.getKey());
				response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
				response.setErrorMsg("");
				response.write(os);
				return;
			}
			
			
		} catch (NameNodeManagerException e) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg("Can not get a data node for query data:" + e.getMessage());
			response.write(os);
			m_logger.error("Can not get a data node for query data.", e);
			return;
		}
	}

}
