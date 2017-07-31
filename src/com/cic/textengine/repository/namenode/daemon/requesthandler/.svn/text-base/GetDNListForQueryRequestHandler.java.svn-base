package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.GetDNAddressForQueryRequest;
import com.cic.textengine.repository.namenode.client.request.GetDNListForQueryRequest;
import com.cic.textengine.repository.namenode.client.response.GetDNListForQueryResponse;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistry;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;

public class GetDNListForQueryRequestHandler implements NNRequestHandler{
	
	private Logger logger = Logger.getLogger(GetDNListForQueryRequestHandler.class);

	@Override
	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		GetDNListForQueryRequest request = new GetDNListForQueryRequest();
		request.read(is);
		
		BufferedOutputStream bos = new BufferedOutputStream(os);
		
		while(request.getYear() != 0){
			GetDNListForQueryResponse response = new GetDNListForQueryResponse();
			ArrayList<String> dnlist = new ArrayList<String>();
			
			try {
				ArrayList<String> dnkey_list = NameNodeManagerFactory
						.getNameNodeManagerInstance().listPartitionDNForQuery(
								request.getYear(), request.getMonth(),
								request.getSiteID(), request.getForumID());
				if(dnkey_list == null || dnkey_list.size() == 0){
					response.setErrorCode(NameNodeConst.ERROR_GENERAL);
					response.setErrorMsg("No active data node can be found for querying data from parititon.");
					response.write(bos);
					bos.flush();
					logger.error("No active data node can be found for querying data from parititon.");
				}else{
					for(String dnKey : dnkey_list){
						DNRegistry dnregistry  = DNRegistryTable.getInstance().getDNRegistry(dnKey);
						if(dnregistry == null)
							continue;
						dnlist.add(dnregistry.getHost()+":"+dnregistry.getPort());
					}
					response.setDnList(dnlist);
					response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
					response.setErrorMsg("");
					response.write(bos);
					bos.flush();
				}
			} catch (NameNodeManagerException e) {
				response.setErrorCode(NameNodeConst.ERROR_GENERAL);
				response.setErrorMsg("Can not get a data node for query data:" + e.getMessage());
				response.write(bos);
				bos.flush();
				logger.error("Can not get a data node for query data.", e);
			}
			
			try {
				byte[] buff = new byte[1];
				is.read(buff,0,1);
				request = new GetDNListForQueryRequest();
				request.read(is);
			} catch(IOException e)
			{
				logger.error("Connection close from client.");
				return;
			}
		}
		
	}

}
