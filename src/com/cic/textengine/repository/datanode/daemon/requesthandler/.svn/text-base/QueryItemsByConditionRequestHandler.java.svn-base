package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.QueryItemsByConditionRequest;
import com.cic.textengine.repository.datanode.client.response.QueryItemsByConditionResponse;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

public class QueryItemsByConditionRequestHandler implements DNRequestHandler{
	
	private static Logger logger = Logger.getLogger(QueryItemsByConditionRequestHandler.class);

	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		QueryItemsByConditionRequest request = new QueryItemsByConditionRequest();
		request.read(is);
		
		// build the query condition
		String conStr = request.getCondition();
		Condition cond = new Condition(conStr);

		int year = request.getYear();
		int month = request.getMonth();
		String siteid = request.getSiteID();
		String forumid = request.getForumID();
		
		// obtain the items 
		ArrayList<TEItem> itemList = new ArrayList<TEItem>();
		try {
			PartitionEnumerator enu = requestContext.getDaemon().getRepositoryEngine().getPartitionEnumerator(year, month, siteid, forumid);
			while(enu.next()) {
				TEItem item = enu.getItem();
				if(cond.match(item))
				{
					itemList.add(item);
				}
			}
			enu.close();
		} catch (RepositoryEngineException e) {
			QueryItemsByConditionResponse response = new QueryItemsByConditionResponse();
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg("Fail to load partition enumerator and enumerate items"+ e.getMessage());
			logger.error("Fail to load partition enumerator and enumerate items"+ e.getMessage());
			response.write(os);
			return;
		}
		
		QueryItemsByConditionResponse response = new QueryItemsByConditionResponse();
		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setItemList(itemList);
		response.setItemCount(itemList.size());
		response.write(os);
		return;
	}

}
