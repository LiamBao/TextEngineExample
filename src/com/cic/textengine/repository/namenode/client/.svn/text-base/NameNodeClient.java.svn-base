package com.cic.textengine.repository.namenode.client;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.namenode.client.request.ApplyPartitionWriteLockRequest;
import com.cic.textengine.repository.namenode.client.request.CleanNameNodeCacheRequest;
import com.cic.textengine.repository.namenode.client.request.CleanPartitionRequest;
import com.cic.textengine.repository.namenode.client.request.DeactivateDataNodeRequest;
import com.cic.textengine.repository.namenode.client.request.GetDNAddressForAppendRequest;
import com.cic.textengine.repository.namenode.client.request.GetDNAddressForQueryRequest;
import com.cic.textengine.repository.namenode.client.request.GetDNClientForQueryRequest;
import com.cic.textengine.repository.namenode.client.request.GetDNClientForWritingRequest;
import com.cic.textengine.repository.namenode.client.request.GetDNListForQueryRequest;
import com.cic.textengine.repository.namenode.client.request.GetDNPartitionItemCountRequest;
import com.cic.textengine.repository.namenode.client.request.GetNextDNPartitionOperationRequest;
import com.cic.textengine.repository.namenode.client.request.ReleasePartitionWriteLockRequest;
import com.cic.textengine.repository.namenode.client.request.UpdateDNPartitionVersionRequest;
import com.cic.textengine.repository.namenode.client.response.ApplyPartitionWriteLockResponse;
import com.cic.textengine.repository.namenode.client.response.CleanNameNodeCacheResponse;
import com.cic.textengine.repository.namenode.client.response.CleanPartitionResponse;
import com.cic.textengine.repository.namenode.client.response.DeactivateDataNodeResponse;
import com.cic.textengine.repository.namenode.client.response.GetDNAddressForAppendResponse;
import com.cic.textengine.repository.namenode.client.response.GetDNAddressForQueryResponse;
import com.cic.textengine.repository.namenode.client.response.GetDNClientForQueryResponse;
import com.cic.textengine.repository.namenode.client.response.GetDNClientForWritingResponse;
import com.cic.textengine.repository.namenode.client.response.GetDNListForQueryResponse;
import com.cic.textengine.repository.namenode.client.response.GetDNPartitionItemCountResponse;
import com.cic.textengine.repository.namenode.client.response.GetNextDNPartitionOperationResponse;
import com.cic.textengine.repository.namenode.client.response.ReleasePartitionWriteLockResponse;
import com.cic.textengine.repository.namenode.client.response.UpdateDNPartitionVersionResponse;
import com.cic.textengine.repository.namenode.client.type.PartitionOperation;
import com.cic.textengine.repository.type.PartitionKey;


public class NameNodeClient {
	String host = null;
	int port = 0;
	Socket daemon_socket = null;
	
	public NameNodeClient(String host, int port){
		this.setHost(host);
		this.setPort(port);
	}

	
	Socket connectNameNodeDaemon() throws NameNodeClientException{
		Socket socket;
		try {
			socket = new Socket(this.getHost(), this.getPort());
		} catch (UnknownHostException e) {
			throw new NameNodeClientException(e);
		} catch (IOException e) {
			throw new NameNodeClientException(e);
		}
		return socket;
	}

	
	/**
	 * Deactivate the data node from the name node.
	 * @param dnkey
	 */
	public void deactivateDataNode(String dnkey){
		if (dnkey == null)
			dnkey = "";
		Socket socket = null;
		try {
			socket = connectNameNodeDaemon();
			DeactivateDataNodeRequest request = new DeactivateDataNodeRequest();
			request.setDNKey(dnkey);

			request.write(socket.getOutputStream());
			
			DeactivateDataNodeResponse response = 
				new DeactivateDataNodeResponse();
			response.read(socket.getInputStream());
		} catch (IOException e) {
		} catch (NameNodeClientException e) {
		} finally{
			try {
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}				
	}
	
	/**
	 * Get the item count for a partition on a data node.
	 * @param dn_key
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeClientException
	 */
	public synchronized long getDNPartitionItemCount(String dn_key,int year, int month,
			 String siteid, String forumid) throws NameNodeClientException{
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			GetDNPartitionItemCountRequest request = new GetDNPartitionItemCountRequest();
			
			request.setForumID(forumid);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setYear(year);
			request.setDNKey(dn_key);

			request.write(socket.getOutputStream());
			
			GetDNPartitionItemCountResponse response = 
				new GetDNPartitionItemCountResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode()!= NameNodeConst.ERROR_SUCCESS){
				throw new NameNodeClientException(response.getErrorMsg());
			}
			
			return response.getItemCount();
		} catch (IOException e) {
			throw new NameNodeClientException(e);
		} finally{
			try {
				socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}		
	}
	
	public synchronized long applyPartitionWriteLock4Append(String dn_key, String dndaemon_ip,
			int year, int month, String siteID, String forumID) throws NameNodeClientException{
		ApplyPartitionWriteLockResponse response = this
				.applyPartitionWriteLock(dn_key, dndaemon_ip, year, month,
						siteID, forumID, 1, null, false);
		if (response.getErrorCode()!=NameNodeConst.ERROR_SUCCESS){
			throw new NameNodeClientException(response.getErrorMsg());
		}
		return response.getStartItemID();
	}
	
	public synchronized void applyPartitionWriteLock4Clean(String dn_key,
			String dndaemon_ip, int year, int month, String siteID,
			String forumID) throws NameNodeClientException {
		ApplyPartitionWriteLockResponse response = this
				.applyPartitionWriteLock(dn_key, dndaemon_ip, year, month,
						siteID, forumID, 2, null, false);
		if (response.getErrorCode() != NameNodeConst.ERROR_SUCCESS) {
			throw new NameNodeClientException(response.getErrorMsg());
		}
	}
	
	public synchronized int applyPartitionWriteLock4Delete(String dn_key,
			String dndaemon_ip, int year, int month, String siteID,
			String forumID, ArrayList<Long> itemIDList, boolean sorted)
			throws NameNodeClientException {
		ApplyPartitionWriteLockResponse response = this
				.applyPartitionWriteLock(dn_key, dndaemon_ip, year, month,
						siteID, forumID, 3, itemIDList, sorted);
		if (response.getErrorCode() != NameNodeConst.ERROR_SUCCESS) {
			throw new NameNodeClientException(response.getErrorMsg());
		}
		return response.getTargetVersion();
	}

	/**
	 * Apply the parition write lock.
	 * 
	 * @param dn_key
	 * @param dndaemon_ip
	 * @param year
	 * @param month
	 * @param siteID
	 * @param forumID
	 * @param operation
	 * @param itemIDList
	 * @param sorted
	 * @return
	 * @throws NameNodeClientException
	 */
	synchronized ApplyPartitionWriteLockResponse applyPartitionWriteLock(String dn_key, String dndaemon_ip,
			int year, int month, String siteID, String forumID, int operation, ArrayList<Long> itemIDList,  boolean sorted) 
	throws NameNodeClientException {
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			ApplyPartitionWriteLockRequest request = new ApplyPartitionWriteLockRequest();
			
			request.setDNKey(dn_key);
			request.setForumID(forumID);
			request.setMonth(month);
			request.setSiteID(siteID);
			request.setYear(year);
			request.setOperation(operation);
			if (itemIDList == null){
				itemIDList = new ArrayList<Long>();
			}
			request.addItemIDList(itemIDList);
			request.setSorted(sorted);
			
			request.write(socket.getOutputStream());
			
			
			ApplyPartitionWriteLockResponse response = 
				new ApplyPartitionWriteLockResponse();
			response.read(socket.getInputStream());
			return response;
		} catch (IOException e) {
			throw new NameNodeClientException(e);
		} finally{
			try {
				socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}
	}

	public DataNodeClient getDNClientForQuery(PartitionKey key) throws NameNodeClientException {
		return getDNClientForQuery(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID());
	}
	/**
	 * Get a data node which can be used to query a partition
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeClientException
	 */
	public DataNodeClient getDNClientForQuery(int year, int month,
			 String siteid, String forumid) throws NameNodeClientException {
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			GetDNClientForQueryRequest request = new GetDNClientForQueryRequest();
			
			request.setForumID(forumid);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setYear(year);

			request.write(socket.getOutputStream());
			
			GetDNClientForQueryResponse response = 
				new GetDNClientForQueryResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode()!= NameNodeConst.ERROR_SUCCESS){
				throw new NameNodeClientException(response.getErrorMsg());
			}
			
			DataNodeClient client = new DataNodeClient(response.getHost(), response.getPort());
			client.setNNClient(this);
			client.setDNKey(response.getDNKey());
			return client;
		} catch (IOException e) {
			throw new NameNodeClientException(e);
		} finally{
			try {
				socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}		
	}
	public String getDNAddressForAppend(int year, int month, String siteid, String forumid) throws NameNodeClientException {
		if(daemon_socket == null)
		{
			daemon_socket = connectNameNodeDaemon();
		}
		try {
			GetDNAddressForAppendRequest request = new GetDNAddressForAppendRequest();
			request.setSiteID(siteid);
			request.setForumID(forumid);
			request.setYear(year);
			request.setMonth(month);
			BufferedOutputStream bos = new BufferedOutputStream(daemon_socket.getOutputStream());
			request.write(bos);
			bos.flush();
			
			GetDNAddressForAppendResponse response = new GetDNAddressForAppendResponse();
			response.read(daemon_socket.getInputStream());
			
			String hostAddress = response.getHost();
			int hostPort = response.getPort();
			String result = hostAddress+":"+hostPort;
			return result;
		} catch (IOException e)
		{
			throw new NameNodeClientException(e);
		} finally {
			if(year == 0)
			{
				try {
					daemon_socket.close();
				} catch (IOException e) {
					// ignore
				} finally {
					daemon_socket = null;
				}
			}
		}
	}
	
	public ArrayList<String> getDNListForQuery(int year, int month, String siteid, String forumid) throws NameNodeClientException{
		if(daemon_socket == null)
		{
			daemon_socket = connectNameNodeDaemon();
		}
		
		try{
			GetDNListForQueryRequest request = new GetDNListForQueryRequest();
			request.setSiteID(siteid);
			request.setForumID(forumid);
			request.setYear(year);
			request.setMonth(month);
			
			BufferedOutputStream bos = new BufferedOutputStream(daemon_socket.getOutputStream());
			request.write(bos);
			bos.flush();
			
			GetDNListForQueryResponse response = new GetDNListForQueryResponse();
			response.read(daemon_socket.getInputStream());
			
			if(response.getErrorCode() != NameNodeConst.ERROR_SUCCESS)
				throw new NameNodeClientException(response.getErrorMsg());
			
			return response.getDnList();
		} catch (IOException e){
			throw new NameNodeClientException(e);
		} finally{
			if(year == 0)
			{
				try {
					daemon_socket.close();
				} catch (IOException e) {
					// ignore
				} finally {
					daemon_socket = null;
				}
			}
		}
	}
	
//	public ArrayList<String> getDNListForQuery(int year, int month, String siteid, String forumid) throws NameNodeClientException{
//		Socket socket = connectNameNodeDaemon();
//
//		try{
//			GetDNListForQueryRequest request = new GetDNListForQueryRequest();
//			request.setSiteID(siteid);
//			request.setForumID(forumid);
//			request.setYear(year);
//			request.setMonth(month);
//			
//			BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
//			request.write(bos);
//			bos.flush();
//			
//			GetDNListForQueryResponse response = new GetDNListForQueryResponse();
//			response.read(socket.getInputStream());
//			
//			if(response.getErrorCode() != NameNodeConst.ERROR_SUCCESS)
//				throw new NameNodeClientException(response.getErrorMsg());
//			
//			return response.getDnList();
//		} catch(IOException e){
//			throw new NameNodeClientException(e);
//		} finally {
//			try {
//				socket.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//
//	}
	public String getDNAddressForQuery(int year, int month, String siteid, String forumid) throws NameNodeClientException {

		if(daemon_socket == null)
		{
			daemon_socket = connectNameNodeDaemon();
		}

		try {
			GetDNAddressForQueryRequest request = new GetDNAddressForQueryRequest();
			
			request.setSiteID(siteid);
			request.setForumID(forumid);
			request.setYear(year);
			request.setMonth(month);
			BufferedOutputStream bos = new BufferedOutputStream(daemon_socket.getOutputStream());
			request.write(bos);
			bos.flush();
			
			GetDNAddressForQueryResponse response = new GetDNAddressForQueryResponse();
			response.read(daemon_socket.getInputStream());
			
			String hostAddress = response.getHost();
			int hostPort = response.getPort();
			String result = hostAddress+":"+hostPort;
			return result;
		} catch (IOException e)
		{
			throw new NameNodeClientException(e);
		} finally {
			if(year == 0)
			{
				try {
					daemon_socket.close();
				} catch (IOException e) {
					// ignore
				} finally {
					daemon_socket = null;
				}
			}
		}
		
		
		
	}
	
	/**
	 * Retrieve the DataNodeClient instance to add new items.
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeClientException  
	 */
	public DataNodeClient getDNClientForWriting(int year, int month,
			 String siteid, String forumid) throws NameNodeClientException {
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			GetDNClientForWritingRequest request = new GetDNClientForWritingRequest();
			
			request.setForumID(forumid);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setYear(year);
			
			request.write(socket.getOutputStream());
			
			GetDNClientForWritingResponse response = 
				new GetDNClientForWritingResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode()!= NameNodeConst.ERROR_SUCCESS){
				throw new NameNodeClientException(response.getErrorMsg());
			}
			
			
			DataNodeClient client = new DataNodeClient(response.getHost(), response.getPort());
			client.setNNClient(this);
			client.setDNKey(response.getDNKey());
			return client;
		} catch (IOException e) {
			throw new NameNodeClientException(e);
		} finally{
			try {
				socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}
	}

	/**
	 * Rlease the parition write lock.
	 * 
	 * This method is only invoked by DataNode daemon
	 * @param dataNodeKey
	 * @param year
	 * @param month
	 * @param siteID
	 * @param forumID
	 * @param operation	The type of operation this release work is handling (1: appending, 2: clean, 3: delete).
	 * @param startItemID  If this operation is append, need to specify the startItemID here
	 * @param itemCount If this operation is append, need to specify the itemCount here
	 * @param targetVersion  If this operation is delete, need to specify the targetVersion here. 
	 * @throws NameNodeClientException
	 */
	public synchronized void cleanPartition(
			int year, int month, String siteID, String forumID)
	throws NameNodeClientException{
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			CleanPartitionRequest request = new CleanPartitionRequest();
			
			request.setForumID(forumID);
			request.setMonth(month);
			request.setSiteID(siteID);
			request.setYear(year);

			request.write(socket.getOutputStream());
			
			CleanPartitionResponse response = 
				new CleanPartitionResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode()!= NameNodeConst.ERROR_SUCCESS){
				throw new NameNodeClientException(response.getErrorMsg());
			}
		} catch (IOException e) {
			throw new NameNodeClientException(e);
		} finally{
			try {
				socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}
	}
	
	public void cleanCache() throws NameNodeClientException{
		Socket socket;
		socket = connectNameNodeDaemon();
		try{
			CleanNameNodeCacheRequest request = new CleanNameNodeCacheRequest();
			request.write(socket.getOutputStream());
			
			CleanNameNodeCacheResponse response = new CleanNameNodeCacheResponse();
			response.read(socket.getInputStream());
			if(response.getErrorCode() != NameNodeConst.ERROR_SUCCESS){
				throw new NameNodeClientException(response.getErrorMsg());
			}
		} catch(IOException e){
			throw new NameNodeClientException(e);
		}finally{
			try {
				socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}
	}
	
	
	/**
	 * Rlease the parition write lock.
	 * 
	 * This method is only invoked by DataNode daemon
	 * @param dataNodeKey
	 * @param year
	 * @param month
	 * @param siteID
	 * @param forumID
	 * @param operation	The type of operation this release work is handling (1: appending, 2: clean, 3: delete).
	 * @param startItemID  If this operation is append, need to specify the startItemID here
	 * @param itemCount If this operation is append, need to specify the itemCount here
	 * @param targetVersion  If this operation is delete, need to specify the targetVersion here. 
	 * @throws NameNodeClientException
	 */
	public synchronized void releasePartitionWriteLock(String dataNodeKey,
			int year, int month, String siteID, String forumID, int operation,
			long startItemID, int itemCount, int targetVersion)
	throws NameNodeClientException{
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			ReleasePartitionWriteLockRequest request = new ReleasePartitionWriteLockRequest();
			
			request.setDNKey(dataNodeKey);
			request.setForumID(forumID);
			request.setMonth(month);
			request.setSiteID(siteID);
			request.setYear(year);
			request.setStartItemID(startItemID);
			request.setItemCount(itemCount);
			request.setOperation(operation);
			request.setTargetVersion(targetVersion);

			request.write(socket.getOutputStream());
			
			ReleasePartitionWriteLockResponse response = 
				new ReleasePartitionWriteLockResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode()!= NameNodeConst.ERROR_SUCCESS){
				throw new NameNodeClientException(response.getErrorMsg());
			}
		} catch (IOException e) {
			throw new NameNodeClientException(e);
		} finally{
			try {
				socket.close();
			} catch (IOException e) {
				//ignore here
			}
		}
	}

	/**
	 * Update a data node partition version.
	 * @param dataNodeKey
	 * @param year
	 * @param month
	 * @param siteID
	 * @param forumID
	 * @param version
	 * @throws NameNodeClientException 
	 * @throws DataNodeClientCommunicationException 
	 */
	public void updateDNPartitionVersion(String dataNodeKey, int year,
			int month, String siteID, String forumID, long itemCount, int version) throws NameNodeClientException, DataNodeClientCommunicationException{
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			UpdateDNPartitionVersionRequest request = new UpdateDNPartitionVersionRequest();

			request.setDNKey(dataNodeKey);
			request.setForumID(forumID);
			request.setMonth(month);
			request.setSiteID(siteID);
			request.setYear(year);
			request.setVersion(version);
			request.setItemCount(itemCount);

			request.write(socket.getOutputStream());

			UpdateDNPartitionVersionResponse response = new UpdateDNPartitionVersionResponse();

			response.read(socket.getInputStream());
			if (response.getErrorCode() != NameNodeConst.ERROR_SUCCESS) {
				throw new NameNodeClientException(response.getErrorMsg());
			}
		} catch (IOException e) {
			throw new DataNodeClientCommunicationException(e);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				// ignore here
			}
		}		
	}

	/**
	 * Get the next operation on the partition for data node. This is for data node
	 * synchronization operation.
	 * 
	 * @param dataNodeKey
	 * @param year
	 * @return
	 */
	public PartitionOperation getNextDNPartitionOperation(String dataNodeKey, int year,
			int month, String siteID, String forumID)
			throws NameNodeClientException, DataNodeClientCommunicationException {
		Socket socket;
		socket = connectNameNodeDaemon();
		try {
			GetNextDNPartitionOperationRequest request = new GetNextDNPartitionOperationRequest();

			request.setDNKey(dataNodeKey);
			request.setForumID(forumID);
			request.setMonth(month);
			request.setSiteID(siteID);
			request.setYear(year);

			request.write(socket.getOutputStream());

			GetNextDNPartitionOperationResponse response = new GetNextDNPartitionOperationResponse();

			response.read(socket.getInputStream());
			if (response.getErrorCode() != NameNodeConst.ERROR_SUCCESS) {
				throw new NameNodeClientException(response.getErrorMsg());
			}
			
			PartitionOperation po = new PartitionOperation();
			po.setYear(year);
			po.setMonth(month);
			po.setSiteID(siteID);
			po.setForumID(forumID);
			po.setOperation(response.getOperation());
			po.setVersion(response.getVersion());
			switch(response.getOperation()){
			case 1://partition add
				po.setStartItemID(response.getStartItemID());
				po.setItemCount(response.getItemCount());
				po.setSeedDNHost(response.getSeedDNHost());
				po.setSeedDNPort(response.getSeedDNPort());
				break;
			case 3://partition delete
				po.setDeleteItemIDListSorted(response.isSorted());
				po.addDeletedItemIDList(response.listItemIDs());
				break;
			}
			
			return po;
		} catch (IOException e) {
			throw new DataNodeClientCommunicationException(e);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				// ignore here
			}
		}
	}



	
	/**
	 * Validate if the data node is validated node for a particular data partition
	 * @param dataNodeKey
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 */
	public boolean validateDataNode(String dataNodeKey, int year, int month,
			String siteid, String forumid) {
		// TODO Auto-generated method stub
		return false;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}




}
