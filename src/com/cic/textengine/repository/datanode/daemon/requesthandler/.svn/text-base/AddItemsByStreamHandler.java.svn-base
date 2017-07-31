package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.TEItemInputStream;
import com.cic.textengine.repository.datanode.client.request.AddItemsByStreamRequest;
import com.cic.textengine.repository.datanode.client.response.AddItemsByStreamResponse;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.partitionlock.IllegalPartitionLockStatusException;
import com.cic.textengine.repository.partitionlock.NoPartitionLockFoundException;
import com.cic.textengine.repository.partitionlock.PartitionAlreadyLockedException;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;
import com.cic.textengine.type.TEItem;

public class AddItemsByStreamHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(AddItemsByStreamHandler.class);
	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		AddItemsByStreamRequest request = new AddItemsByStreamRequest();
		request.read(is);	
		
		NameNodeClient nn_client = new NameNodeClient(requestContext
				.getDaemon().getNNDaemonIP(), requestContext.getDaemon()
				.getNNDaemonPort());
		//check if this data node is already registered on name node
		
		if (requestContext.getDaemon().getNNDaemonIP()==null){
			AddItemsByStreamResponse response = new AddItemsByStreamResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Data node unknown for name node.");
			response.write(os);
			return;
		}

		
		//check local partition write lock on data node.
		PartitionWriteLockManager pwlm = null;
		pwlm = PartitionWriteLockManager.getInstance();
		try {
			pwlm.applyLock(request.getYear(), request.getMonth(), request
					.getSiteID(), request.getForumID(), requestContext
					.getSocket().getInetAddress().getHostAddress(),
					requestContext.getDaemon().getDataNodeKey(), 1);
		} catch (PartitionAlreadyLockedException e) {
			AddItemsByStreamResponse response = new AddItemsByStreamResponse();
			response.setErrorCode(DataNodeConst.ERROR_DN_UNKNOWN_TO_NN);
			response.setErrorMsg("Can't apply the local parition write lock.");
			response.write(os);
			
			m_logger.error("Error", e);
			return;
		}

		long startItemID = -1;
		//apply partition write lock from name node
		try {
			startItemID = nn_client.applyPartitionWriteLock4Append(requestContext.getDaemon().getDataNodeKey(), 	//data node key
					requestContext.getSocket().getLocalAddress().toString(),	//ip address 
					request.getYear(),
					request.getMonth(), 
					request.getSiteID(), 
					request.getForumID());
		} catch (NameNodeClientException e1) {
			AddItemsByStreamResponse response = new AddItemsByStreamResponse();
			response.setErrorCode(DataNodeConst.ERROR_IDFENGINE);
			response.setErrorMsg("Can not apply for partition write lock because of:" + e1.getMessage());
			response.write(os);
			
			m_logger.debug("Can't apply for partition write lock.", e1);
			
			//release the local lock
			try {
				pwlm.releaseLock(request.getYear(), request.getMonth(), request
						.getSiteID(), request.getForumID(), requestContext
						.getSocket().getInetAddress().getHostAddress(),
						requestContext.getDaemon().getDataNodeKey(), 0);
			} catch (NoPartitionLockFoundException e) {
				//should not happen
				m_logger.error("Exception", e1);
			} 
			catch (IllegalPartitionLockStatusException e) {
				//should not happen
				m_logger.error("Exception", e1);
			}
			return;
		}
		
		
		boolean succ = false;
		int item_count = 0;
		try{
			item_count = processRequest(nn_client, startItemID, requestContext, is, os,
					request, pwlm);
			m_logger.debug(item_count + " items have been added to the data node repository.");
			succ = true;
		}catch(Exception e){
			item_count = 0;
			m_logger.error("Error", e);
		}finally{
			//release the NN lock. It MUST be released before releasing the local partition lock.
			try {
				nn_client.releasePartitionWriteLock(requestContext.getDaemon()
						.getDataNodeKey(), request.getYear(), request.getMonth(),
						request.getSiteID(), request.getForumID(),
						succ?1:0,
						startItemID,
						item_count,
						0
						);
			} catch (NameNodeClientException e) {
				item_count = 0;
				m_logger.error("Error", e);
			}

			//release the local lock
			try {
				pwlm.releaseLock(request.getYear(), request.getMonth(), request
						.getSiteID(), request.getForumID(), requestContext
						.getSocket().getInetAddress().getHostAddress(),
						requestContext.getDaemon().getDataNodeKey(), succ?1:0);
			} catch (NoPartitionLockFoundException e1) {
				item_count = 0;
				m_logger.error("Exception", e1);
			} catch (IllegalPartitionLockStatusException e1) {
				item_count = 0;
				m_logger.error("Exception", e1);
			}
		}
		
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeLong(startItemID);
		dos.writeInt(item_count);
		
		return;
	}

	/**
	 * @param requestContext
	 * @param is
	 * @param os
	 * @param request
	 * @param pwlm
	 * @return Number of items returned.
	 * @throws IOException
	 * @throws RepositoryEngineException 
	 */
	private int processRequest(NameNodeClient nn_client,long startItemID, DNRequestContext requestContext,
			InputStream is, OutputStream os, AddItemsByStreamRequest request,
			PartitionWriteLockManager pwlm) throws IOException, RepositoryEngineException , EOFException{

		PartitionWriter writer = null;
		writer = requestContext.getDaemon()
				.getRepositoryEngine().getPartitionWriter(request.getYear(),
						request.getMonth(), request.getSiteID(),
						request.getForumID(), startItemID);
 
		
		//send success signal , start receiving items
		AddItemsByStreamResponse response = new AddItemsByStreamResponse();
		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");
		response.write(os);
		
		
		DataInputStream dis = new DataInputStream(is);
		TEItemInputStream teis = new TEItemInputStream(is);
		
		byte control_byte = 0x00;
		control_byte = dis.readByte();
		TEItem item = null;
		int item_count = 0;
		while (control_byte == 0x01) {
			item = teis.readItem();
			control_byte = dis.readByte();
			writer.writeItem(item);
			item_count++;
		}
		writer.close();
		return item_count;
	}
}
