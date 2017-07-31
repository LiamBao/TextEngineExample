package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.QueryItemsRequest;
import com.cic.textengine.repository.datanode.client.response.QueryItemsResponse;
import com.cic.textengine.repository.datanode.repository.PartitionSearcher;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.type.TEItem;

public class QueryItemsRequestHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(QueryItemsRequestHandler.class);

	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		QueryItemsRequest request = new QueryItemsRequest();
		request.read(is);

		//check if this data node is already registered on name node
		if (requestContext.getDaemon().getNNDaemonIP()==null){
			QueryItemsResponse response = new QueryItemsResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Data node unknown for name node.");
			response.write(os);
			return;
		}
		
		NameNodeClient nn_client = new NameNodeClient(requestContext
				.getDaemon().getNNDaemonIP(), requestContext.getDaemon()
				.getNNDaemonPort());
		long item_count;
		try {
			item_count = nn_client.getDNPartitionItemCount(requestContext.getDaemon()
					.getDataNodeKey(), request.getYear(), request.getMonth(),
					request.getSiteID(), request.getForumID());
		} catch (NameNodeClientException e) {
			QueryItemsResponse response = new QueryItemsResponse();
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg("Failed to query item count for partition from Name Node.");
			response.write(os);
			m_logger.error("Exception", e);
			return;
		}
		
		PartitionSearcher ps;
		ArrayList<TEItem> result = new ArrayList<TEItem>();
		
		QueryItemsResponse response = new QueryItemsResponse();
		try {
			//sort item id list
			ArrayList<Long> id_list = null;
			if (!request.isSorted()){
				id_list = new ArrayList<Long>(request.listItemIDs());
				Collections.sort(id_list);
			}else{
				id_list = request.listItemIDs();
			}
			ArrayList<Long> filtered_list = null;
			if (id_list.get(id_list.size()-1) <= item_count){
				filtered_list = id_list;
			}else{
				int idx = 0;
				filtered_list = new ArrayList<Long>();
				while(id_list.get(idx) <= item_count){
					filtered_list.add(id_list.get(idx));
					idx++;
				}
			}
						
			
			ps = requestContext.getDaemon().getRepositoryEngine().getPartitionSearcher(
					request.getYear(), request.getMonth(), request.getSiteID(),
					request.getForumID());
			result = ps.queryItems(filtered_list, true);
		} catch (RepositoryEngineException e) {
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg("Failed to query items from partition:" + e.getMessage());
			response.write(os);
			m_logger.error("Exception", e);
			return;
		}

		response.addTEItemList(result);
		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");
		response.write(os);
	}
}
