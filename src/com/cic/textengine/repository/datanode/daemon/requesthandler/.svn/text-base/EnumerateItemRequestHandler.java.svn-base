package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataOutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.TEItemOutputStream;
import com.cic.textengine.repository.datanode.client.request.EnumerateItemRequest;
import com.cic.textengine.repository.datanode.client.response.EnumerateItemResponse;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;

public class EnumerateItemRequestHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(EnumerateItemRequestHandler.class);
	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		EnumerateItemRequest request = new EnumerateItemRequest();
		request.read(is);	
		
		//check if this data node is already registered on name node
		if (requestContext.getDaemon().getNNDaemonIP()==null){
			EnumerateItemResponse response = new EnumerateItemResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Data node unknown for name node.");
			response.write(os);
			m_logger.error("Data node unknown for name node.");
			return;
		}

		NameNodeClient nn_client = new NameNodeClient(requestContext
				.getDaemon().getNNDaemonIP(), requestContext.getDaemon()
				.getNNDaemonPort());
		long item_count = 0;
		try {
			item_count = nn_client.getDNPartitionItemCount(requestContext.getDaemon()
					.getDataNodeKey(), request.getYear(), request.getMonth(),
					request.getSiteID(), request.getForumID());
		} catch (NameNodeClientException e1) {
			EnumerateItemResponse response = new EnumerateItemResponse();
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg("Can not retrieve parition item count for data node.");
			response.write(os);
			m_logger.error("Exception", e1);
			return;
		}
		
		
		EnumerateItemResponse response = new EnumerateItemResponse();
		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("Success, start feeding items");
		if (request.getItemCount() < item_count && request.getItemCount() > 0)
			item_count = request.getItemCount();
		response.setItemCount(item_count);
		response.write(os);
		
		DataOutputStream dos = new DataOutputStream(os);
		TEItemOutputStream teos = new TEItemOutputStream(os);

		
		try {
			PartitionEnumerator enu = requestContext.getDaemon()
					.getRepositoryEngine().getPartitionEnumerator(
							request.getYear(), request.getMonth(),
							request.getSiteID(), request.getForumID(),
							request.getStartItemID(),
							request.isIncludeDeletedItems());
			
			while(enu.next() && item_count > 0){
				dos.writeByte(0x01);
				teos.writeTEItem(enu.getItem());
				item_count--;
			}
			dos.writeByte(0x00);
		} catch (RepositoryEngineException e) {
			m_logger.error("Exception", e);
		}
		
		return;
	}
}
