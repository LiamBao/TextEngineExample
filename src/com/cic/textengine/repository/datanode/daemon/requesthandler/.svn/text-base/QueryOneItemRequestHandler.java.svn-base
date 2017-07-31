package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.QueryOneItemRequest;
import com.cic.textengine.repository.datanode.client.response.QueryOneItemResponse;
import com.cic.textengine.repository.datanode.repository.PartitionSearcher;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class QueryOneItemRequestHandler implements DNRequestHandler{
	
	private static Logger m_logger = Logger.getLogger(QueryOneItemRequestHandler.class);

	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		HashMap<String, PartitionSearcher> psMap = new HashMap<String, PartitionSearcher>();
//		HashMap<String, Long> itemCountMap = new HashMap<String, Long>();
		
		QueryOneItemRequest request = new QueryOneItemRequest();
		request.read(is);
		BufferedOutputStream bos = new BufferedOutputStream(os);
		
		QueryOneItemResponse response = null;
		
		// if itemID = 0, means query no more items.
//		long item_count = 0;
		long itemid = request.getItemID();
		boolean m_stop = false;
		
		if(itemid == 0)
			return;
		
		String current_parKey = (new PartitionKey(request.getYear(),
				request.getMonth(), request.getSiteID(), request.getForumID()))
				.generateStringKey();
		PartitionSearcher ps = null;
		
		//check if this data node is already registered on name node
		if (requestContext.getDaemon().getNNDaemonIP()==null){
			response = new QueryOneItemResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Data node unknown for name node.");
			response.write(bos);
			return;
		}
			
//		NameNodeClient nn_client = new NameNodeClient(requestContext
//				.getDaemon().getNNDaemonIP(), requestContext.getDaemon()
//				.getNNDaemonPort());
//		try {
//			item_count = nn_client.getDNPartitionItemCount(requestContext.getDaemon()
//					.getDataNodeKey(), request.getYear(), request.getMonth(),
//					request.getSiteID(), request.getForumID());
//			itemCountMap.put(current_parKey, item_count);
//		} catch (NameNodeClientException e) {
//			response = new QueryOneItemResponse();
//			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
//			response.setErrorMsg("Failed to query item count for partition from Name Node.");
//			response.write(bos);
//			bos.flush();
//			m_logger.error("Exception", e);
//			return;
//		}
		try {
			ps = requestContext.getDaemon().getRepositoryEngine().getPartitionSearcher(
					request.getYear(), request.getMonth(), request.getSiteID(),
					request.getForumID());
			psMap.put(current_parKey, ps);
		} catch (RepositoryEngineException e) {
			response = new QueryOneItemResponse();
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg("Failed to query items from partition:" + e.getMessage());
			response.write(bos);
			bos.flush();
			m_logger.error("Exception", e);
			return;
		}
		while(!m_stop) {
			
			TEItem result = null;
			try {
//				if(itemid > item_count)
//				{
//					response = new QueryOneItemResponse();
//					response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
//					response.setErrorMsg("The itemID is out of range");
//					response.write(bos);
//					bos.flush();
//					m_logger.error("The itemID is out of range.");
//					return;
//				} else {
					result = ps.queryItem(itemid);
//				}
			} catch (RepositoryEngineException e) {
				response = new QueryOneItemResponse();
				response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
				response.setErrorMsg("Failed to query items from partition:" + e.getMessage());
				response.write(bos);
				bos.flush();
				m_logger.error("Exception", e);
				return;
			}
			
			if(result == null)
			{
				response = new QueryOneItemResponse();
				response.setErrorCode(DataNodeConst.ERROR_IDFENGINE);
				response.setErrorMsg("Item does not existed, could be deleted.");
				response.write(bos);
				bos.flush();
				m_logger.error("Exception: item does not existed, could be deleted.");
				return;
			}
			
			response = new QueryOneItemResponse();
			response.setItem(result);
			response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("");
			response.write(bos);
			bos.flush();
			
			try {
				byte[] buff = new byte[1];
				is.read(buff,0,1);
				request = new QueryOneItemRequest();
				request.read(is);
			} catch(IOException e)
			{
				m_logger.error("Connection closed from client.");
				return;
			}
			itemid = request.getItemID();

			
			if(itemid == 0)
			{
				m_logger.debug(String.format("%s partitions here.", psMap.keySet().size()));
				return;
			}
			
			String parKey = (new PartitionKey(request.getYear(), request
					.getMonth(), request.getSiteID(), request.getForumID()))
					.generateStringKey();
//			if(!temp_parKey.equals(current_parKey))
			if(!psMap.keySet().contains(parKey))
			{
//				try {
//					item_count = nn_client.getDNPartitionItemCount(requestContext.getDaemon()
//							.getDataNodeKey(), request.getYear(), request.getMonth(),
//							request.getSiteID(), request.getForumID());
//					itemCountMap.put(parKey, item_count);
//				} catch (NameNodeClientException e) {
//					response = new QueryOneItemResponse();
//					response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
//					response.setErrorMsg("Failed to query item count for partition from Name Node.");
//					response.write(bos);
//					bos.flush();
//					m_logger.error("Exception", e);
//					return;
//				}
				
				try {
					ps = requestContext.getDaemon().getRepositoryEngine().getPartitionSearcher(
							request.getYear(), request.getMonth(), request.getSiteID(),
							request.getForumID());
					psMap.put(parKey, ps);
				} catch (RepositoryEngineException e) {
					response = new QueryOneItemResponse();
					response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
					response.setErrorMsg("Failed to query items from partition:" + e.getMessage());
					response.write(bos);
					bos.flush();
					m_logger.error("Exception", e);
					return;
				}
			}
			
			
//			item_count = itemCountMap.get(parKey);
			ps = psMap.get(parKey);
		}
	}

}
