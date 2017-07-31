package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.GetDNAddressForAppendRequest;
import com.cic.textengine.repository.namenode.client.response.GetDNAddressForAppendResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.exception.NoDataNodeAvaliableForPartitionWrite;
import com.cic.textengine.repository.namenode.manager.type.DataNode;
import com.cic.textengine.repository.namenode.strategy.DNChooseStrategyFactory;

public class GetDNAddressForAppendRequestHandler implements NNRequestHandler{

	private static Logger m_logger = Logger.getLogger(GetDNAddressForAppendRequestHandler.class);
	
	@Override
	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		
		GetDNAddressForAppendRequest request = new GetDNAddressForAppendRequest();
		request.read(is);

		BufferedOutputStream bos = new BufferedOutputStream(os);
		
		while(request.getYear() != 0)
		{
			GetDNAddressForAppendResponse response = new GetDNAddressForAppendResponse();
			
			try {
				ArrayList<String> dnkey_list = NameNodeManagerFactory
						.getNameNodeManagerInstance().getDataNodeForPartitionWrite(
								request.getYear(), request.getMonth(),
								request.getSiteID(), request.getForumID());

				DataNode dn = DNChooseStrategyFactory.getDNChooseStrategyInstance()
						.chooseDNForPartitionWrite(request.getYear(), request.getMonth(), request.getSiteID(), request.getForumID(), dnkey_list);
				
				if(dn == null){
					response.setErrorCode(NameNodeConst.ERROR_GENERAL);
					response.setErrorMsg("No active data node can be found for appending data from parititon.");
					response.write(bos);
					bos.flush();
					m_logger.error("No active data node can be found for appending data from parititon.");
				}else{
					response.setHost(dn.getIP());
					response.setPort(dn.getPort());
					response.setDNKey(dn.getKey());
					response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
					response.setErrorMsg("");
					response.write(bos);
					bos.flush();
				}
				
				
			} catch (NameNodeManagerException e) {
				response.setErrorCode(NameNodeConst.ERROR_GENERAL);
				response.setErrorMsg("Can not get a data node for append data:" + e.getMessage());
				response.write(bos);
				bos.flush();
				m_logger.error("Can not get a data node for append data.", e);
			} catch (NoDataNodeAvaliableForPartitionWrite e) {
				response.setErrorCode(NameNodeConst.ERROR_GENERAL);
				response.setErrorMsg("Can not get a data node for append data:" + e.getMessage());
				response.write(bos);
				bos.flush();
				m_logger.error("Can not get a data node for append data.", e);
			}
			
			try {
				byte[] buff = new byte[1];
				is.read(buff,0,1);
				request = new GetDNAddressForAppendRequest();
				request.read(is);
			} catch(IOException e)
			{
				m_logger.error("Connection close from client.");
				return;
			}
		}
	}

}
