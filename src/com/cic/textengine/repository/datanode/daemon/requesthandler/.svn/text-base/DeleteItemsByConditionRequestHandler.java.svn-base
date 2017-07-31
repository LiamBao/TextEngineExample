package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.DeleteItemsByConditionRequest;
import com.cic.textengine.repository.datanode.client.response.DeleteItemsByConditionResponse;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.partitionlock.IllegalPartitionLockStatusException;
import com.cic.textengine.repository.partitionlock.NoPartitionLockFoundException;
import com.cic.textengine.repository.partitionlock.PartitionAlreadyLockedException;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;
import com.cic.textengine.type.TEItem;

public class DeleteItemsByConditionRequestHandler implements DNRequestHandler{

	private Logger m_logger = Logger.getLogger(DeleteItemsByConditionRequestHandler.class);
	
	@Override
	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		DeleteItemsByConditionRequest request = new DeleteItemsByConditionRequest();
		request.read(is);
		
		//check if this data node is already registered on name node
		if (requestContext.getDaemon().getNNDaemonIP()==null){
			DeleteItemsByConditionResponse response = new DeleteItemsByConditionResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Data node unknown for name node.");
			response.write(os);
			return;
		}
		
		NameNodeClient nn_client = new NameNodeClient(requestContext
				.getDaemon().getNNDaemonIP(), requestContext.getDaemon()
				.getNNDaemonPort());
	
		//check local partition write lock on data node.
		PartitionWriteLockManager pwlm = null;
		pwlm = PartitionWriteLockManager.getInstance();
		try {
			pwlm.applyLock(request.getYear(), request.getMonth(), request
					.getSiteID(), request.getForumID(), requestContext
					.getSocket().getInetAddress().getHostAddress(),
					requestContext.getDaemon().getDataNodeKey(), 3);
		} catch (PartitionAlreadyLockedException e) {
			DeleteItemsByConditionResponse response = new DeleteItemsByConditionResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Can't apply the local parition write lock.");
			response.write(os);
			
			m_logger.error("Error", e);
			return;
		}
		
		int year = request.getYear();
		int month = request.getMonth();
		String siteid = request.getSiteID();
		String forumid = request.getForumID();
		
		// build the query condition
		String conStr = request.getCondition();
		Condition cond = new Condition(conStr);
		
		// obtain all the item ID of items to be deleted 
		ArrayList<Long> itemIDList = new ArrayList<Long>();
		ArrayList<TEItem> itemList = new ArrayList<TEItem>();
		try {
			PartitionEnumerator enu = requestContext.getDaemon().getRepositoryEngine().getPartitionEnumerator(year, month, siteid, forumid);
			while(enu.next()) {
				TEItem item = enu.getItem();
				if(cond.match(item))
				{
					itemIDList.add(item.getMeta().getItemID());
					itemList.add(item);
				}
			}
			enu.close();
		} catch (RepositoryEngineException e) {
			DeleteItemsByConditionResponse response = new DeleteItemsByConditionResponse();
			response.setErrorCode(DataNodeConst.ERROR_IDFENGINE);
			response.setErrorMsg("Can not load partition to enumerate items because of:" + e.getMessage());
			response.write(os);
			
			m_logger.error("Can not load partition to enumerate items.", e);
		}
		
		int targetVersion = -1;
		//apply partition write lock from name node
		try {
			targetVersion = nn_client.applyPartitionWriteLock4Delete(requestContext.getDaemon().getDataNodeKey(), 	//data node key
					requestContext.getSocket().getLocalAddress().toString(),	//ip address 
					request.getYear(),
					request.getMonth(), 
					request.getSiteID(), 
					request.getForumID(),
					itemIDList, true);
		} catch (NameNodeClientException e1) {
			DeleteItemsByConditionResponse response = new DeleteItemsByConditionResponse();
			response.setErrorCode(DataNodeConst.ERROR_IDFENGINE);
			response.setErrorMsg("Can not apply for partition write lock because of:" + e1.getMessage());
			response.write(os);
			
			m_logger.error("Can't apply for partition write lock.", e1);
			
			//release the local lock
			try {
				pwlm.releaseLock(request.getYear(), request.getMonth(), request
						.getSiteID(), request.getForumID(), requestContext
						.getSocket().getInetAddress().getHostAddress(),
						requestContext.getDaemon().getDataNodeKey(), 0);
			} catch (NoPartitionLockFoundException e) {
				//should not happen
			} 
			catch (IllegalPartitionLockStatusException e) {
				//should not happen
			}

			return;
		}		
		
		DeleteItemsByConditionResponse response = new DeleteItemsByConditionResponse();
		boolean succ = false;
		try {
			requestContext.getDaemon().getRepositoryEngine().deleteItems(
					request.getYear(), request.getMonth(), request.getSiteID(),
					request.getForumID(), itemIDList, true);

			response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
			response.setErrorMsg("");
			response.setDeleteCount(itemIDList.size());
			response.setItemList(itemList);
			succ = true;
		} catch (RepositoryEngineException e) {
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg("Failed to delete items from partition:" + e.getMessage());
			m_logger.error("Exception", e);
			return;
		} finally{
			//release the local lock
			try {
				pwlm.releaseLock(request.getYear(), request.getMonth(), request
						.getSiteID(), request.getForumID(), requestContext
						.getSocket().getInetAddress().getHostAddress(),
						requestContext.getDaemon().getDataNodeKey(), succ?3:0);
			} catch (NoPartitionLockFoundException e1) {
			} catch (IllegalPartitionLockStatusException e1) {
			}
			
			//release name node lock (cancel the operation)
			this.releaseNNLock(nn_client, requestContext.getDaemon()
					.getDataNodeKey(), request.getYear(), request.getMonth(),
					request.getSiteID(), request.getForumID(),targetVersion, succ);
		}

		response.write(os);
		
	}
	
	void releaseNNLock(NameNodeClient nn_client, String key, int year,
			int month, String siteid, String forumid,
			int targetVersion, boolean succ) {
		try {
			int operation = 3;
			if (!succ)
				operation = 0;
			nn_client.releasePartitionWriteLock(key, 	//data node key
					year,
					month, 
					siteid, 
					forumid,
					operation,
					0,
					0,
					targetVersion
					);
		} catch (NameNodeClientException e) {
			m_logger.error("Failed to release write lock on name node.", e);
		}		
	}

}

class Condition {
	
	int year = 0;
	int month = 0;
	long siteID = 0;
	String source = null;
	String forumID = null;
	long threadid = -1;
	long extractDate1 = 0;
	long extractDate2 = 0;
	
	public Condition(String conditionStr) {
		String[] subStrs = conditionStr.split("AND");
		for(String subCon : subStrs) {
			String field = subCon.split(":")[0].trim();
			String value = subCon.split(":")[1].trim();
			if(field.equalsIgnoreCase("source")) {
				source = value;
			} else {
				if(field.equalsIgnoreCase("year")) {
					year = Integer.parseInt(value);
				} else {
					if (field.equalsIgnoreCase("month")) {
						month = Integer.parseInt(value);
					} else {
						if (field.equalsIgnoreCase("siteID")) {
							siteID = Long.parseLong(value);
						} else {
							if (field.equalsIgnoreCase("forumID")) {
								forumID = value;
							} else {
								if (field.equalsIgnoreCase("threadID")) {
									threadid = Long.parseLong(value);
								} else {
									if (field
											.equalsIgnoreCase("latestextractiondate")) {
										String sub = value.substring(1, value.length()-1);
										String[] values = sub.split("TO");
										extractDate1 = Long.parseLong(values[0].trim());
										extractDate2 = Long.parseLong(values[1].trim());
									} 
								}
							}
						}
					}
				}
			}
		}
		
		if(extractDate1 !=0 && extractDate1>= extractDate2){
			extractDate2 = System.currentTimeMillis();
		}
	}
	
	public boolean match(TEItem item) {
		if(threadid != -1 && item.getMeta().getThreadID() != threadid)
			return false;
		if(extractDate1 != 0) {
			long date =item.getMeta().getLatestExtractionDate();
			if(date <= extractDate1 || date >= extractDate2)
				return false;
		}
		if(item.getMeta().getYearOfPost() != year) 
			return false;
		if(item.getMeta().getMonthOfPost() != month)
			return false;
		if(item.getMeta().getSiteID() != siteID)
			return false;
		if(!item.getMeta().getForumID().equalsIgnoreCase(forumID))
			return false;
		return true;
	}
}
