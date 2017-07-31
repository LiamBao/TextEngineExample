package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.GetNextDNPartitionOperationRequest;
import com.cic.textengine.repository.namenode.client.response.GetNextDNPartitionOperationResponse;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.type.DataNode;
import com.cic.textengine.repository.namenode.manager.type.OLogItem;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionDelete;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionWrite;
import com.cic.textengine.repository.namenode.strategy.DNChooseStrategyFactory;

public class GetNextDNPartitionOperationRequestHandler implements NNRequestHandler{
	Logger m_logger = Logger.getLogger(GetNextDNPartitionOperationRequestHandler.class);
	
	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {

		GetNextDNPartitionOperationRequest request = new GetNextDNPartitionOperationRequest();
		request.read(is);
		
		
		GetNextDNPartitionOperationResponse response = new GetNextDNPartitionOperationResponse();
		OLogItem item = null;
		try {
			item = NameNodeManagerFactory.getNameNodeManagerInstance()
					.getNextDNPartitionOperation(request.getDNKey(),
							request.getYear(), request.getMonth(),
							request.getSiteID(), request.getForumID());
		} catch (NameNodeManagerException e) {
			response.setErrorCode(NameNodeConst.ERROR_GENERAL);
			response.setErrorMsg(e.getMessage());
			m_logger.error("Exception:", e);
			response.write(os);
			return;
		}
		
		response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
		if (item == null){
			//the partition already has the latest version of partition.
			response.setOperation(0);
		}else{
			response.setOperation(item.getType());
			response.setVersion(item.getVersion());
			response.setPartitionID(item.getPartitionID());
			
			switch(item.getType()){
			case 1:
				response.setStartItemID(((OLogPartitionWrite)item).getStartItemID());
				response.setItemCount(((OLogPartitionWrite)item).getItemCount());

				ArrayList<String> keys = ((OLogPartitionWrite)item).listSeedDNKeys();

				DataNode dn = DNChooseStrategyFactory
						.getDNChooseStrategyInstance()
						.chooseDNForPartitionAddSync(keys);
				if(dn == null){//can't find a DN to sync partition data from. 
					m_logger
							.fatal("No activate data node can be found to sync data from for partition [Y:"
									+ request.getYear()
									+ ",M:"
									+ request.getMonth()
									+ ",S:"
									+ request.getSiteID()
									+ ",F:"
									+ request.getForumID());
					response.setOperation(0);
				}else{
					response.setSeedDNHost(dn.getIP());
					response.setSeedDNPort(dn.getPort());
					response.setSeedDNKey(dn.getKey());
				}
				break;
			case 3:
				response.setSorted(((OLogPartitionDelete)item).isSorted());
				response.addItemIDList(((OLogPartitionDelete)item).listItemIDs());
				break;
			}
		}

		response.write(os);
	}
}
