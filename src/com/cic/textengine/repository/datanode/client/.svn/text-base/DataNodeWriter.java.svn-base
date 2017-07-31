package com.cic.textengine.repository.datanode.client;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.exception.DataNodeWriterException;
import com.cic.textengine.repository.datanode.client.request.PersistAddItemsRequest;
import com.cic.textengine.repository.datanode.client.response.PersistAddItemsResponse;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

public class DataNodeWriter {
	DataNodeClient m_DNClient = null;
	Socket m_socket = null;
	OutputStream m_outputStream = null;
	DataOutputStream m_dataOutputStream = null;
	InputStream m_inputStream = null;
	
	long startItemID = 0;
	public long getStartItemID() {
		return startItemID;
	}

	public void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
	}
	long remoteItemCount = 0; 

	public long getRemoteItemCount() {
		return remoteItemCount;
	}

	public void setRemoteItemCount(long remoteItemCount) {
		this.remoteItemCount = remoteItemCount;
	}
	int count = 0;
	int localCount = 0;
	
//	int year, month;
//	String siteID, forumID;
	
	boolean flag = false;
	/**
	 * 
	 * @param dnClient
	 * @param socket
	 * @throws IOException
	 */
	public DataNodeWriter(DataNodeClient dnClient, Socket socket)
			throws IOException {
		
		//initialize the connection to data node
		m_DNClient = dnClient;
		m_socket = socket;
		m_outputStream = socket.getOutputStream();
		m_inputStream = socket.getInputStream();
		m_dataOutputStream = new DataOutputStream(socket.getOutputStream());
	}
	
	/**
	 * 
	 * @param local_repository_engine
	 * @param itemCount
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @throws RepositoryEngineException
	 * @throws IOException
	 * @throws DataNodeWriterException 
	 */
	public void uploadLocalPartition(RepositoryEngine local_repository_engine,
			long itemCount, int year, int month, String siteid, String forumid)
			throws RepositoryEngineException, IOException, DataNodeWriterException {
		// send the request
		BufferedOutputStream bos = new BufferedOutputStream(m_outputStream);
		PersistAddItemsRequest request = new PersistAddItemsRequest();
		request.setSiteID(siteid);
		request.setForumID(forumid);
		request.setYear(year);
		request.setMonth(month);
		request.write(bos);
		bos.flush();
		
		// read the response to confirm
		PersistAddItemsResponse response = new PersistAddItemsResponse();
		response.read(m_inputStream);
		if(response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
			throw new DataNodeWriterException(response.getErrorMsg());
		}
		
		PartitionEnumerator enu = local_repository_engine
				.getPartitionEnumerator(year, month, siteid, forumid);
		long count = 0;
		while (enu.next() && count < itemCount) {
			this.writeTEItem(enu.getItem());
			count++;
		}
		enu.close();
		if (count != itemCount){
			throw new RepositoryEngineException("Can not read " + itemCount
					+ " items from local Partition.");
		}

		m_dataOutputStream.writeByte(0x00);
		m_dataOutputStream.flush();

		int remoteCount = 0;
		DataInputStream dis = new DataInputStream(m_inputStream);
		this.setStartItemID(dis.readLong());
		this.setRemoteItemCount(dis.readInt());
		
		if (count != itemCount){
			throw new RepositoryEngineException("Number of uploaded items are not matching: [y:" + year
					+ ",m:" + month + ",s:" + siteid + ",f:"
					+ forumid + ", NumOfLocalItems:" + itemCount
					+ ", NumOfWrittenItems:"
					+ remoteCount + ",dn: "
					+ m_DNClient.host + "] ");
		}
	}
	/**
	 * 
	 * @param item
	 * @throws IOException
	 */
	public void writeTEItem(TEItem item) throws IOException {
		m_dataOutputStream.writeByte(0x01);
		item.write(m_dataOutputStream);
		this.localCount++;
	}
	/**
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (m_socket != null){
			m_socket.close();
			m_socket = null;
		}
	}
}
