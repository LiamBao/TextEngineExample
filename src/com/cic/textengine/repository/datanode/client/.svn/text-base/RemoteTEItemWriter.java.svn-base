package com.cic.textengine.repository.datanode.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.cic.textengine.repository.TEWriter;
import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemWriterException;
import com.cic.textengine.repository.datanode.client.request.AddItemsByStreamRequest;
import com.cic.textengine.repository.datanode.client.response.AddItemsByStreamResponse;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

public class RemoteTEItemWriter implements TEWriter {
	DataNodeClient m_DNClient = null;
	Socket m_socket = null;
	OutputStream m_outputStream = null;
	DataOutputStream m_dataOutputStream = null;
	InputStream m_inputStream = null;
	
	long startItemID = 0;
	int count = 0;
	int localCount = 0;
	
	int year, month;
	String siteID, forumID;
	
	RemoteTEItemWriter(DataNodeClient dn_client, Socket socket, int year, int month, String siteid, String forumid) 
	throws IOException, RemoteTEItemWriterException{
		m_DNClient = dn_client;
		
		m_socket = socket;
		m_outputStream = socket.getOutputStream();
		m_inputStream = socket.getInputStream();
		m_dataOutputStream = new DataOutputStream(socket.getOutputStream());
		
		
		
		AddItemsByStreamRequest request = new AddItemsByStreamRequest();
		request.setForumID(forumid);
		request.setSiteID(siteid);
		request.setYear(year);
		request.setMonth(month);
		request.write(m_outputStream);
		
		this.setYear(year);
		this.setMonth(month);
		this.setSiteID(siteid);
		this.setForumID(forumid);
		
		AddItemsByStreamResponse response = new AddItemsByStreamResponse();
		response.read(m_inputStream);
		if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS){
			throw new RemoteTEItemWriterException(response.getErrorMsg());
		}
		
		localCount = 0;
	}
	
	public void close() throws IOException, RemoteTEItemWriterException {
		if (m_socket != null){
			m_dataOutputStream.writeByte(0x00);

			int remoteCount = 0;
			DataInputStream dis = new DataInputStream(m_inputStream);
			this.setStartItemID(dis.readLong());
			remoteCount = dis.readInt();
			m_socket.close();
			m_socket = null;
			
			if (this.localCount != remoteCount){
				throw new RemoteTEItemWriterException("Failed to upload items to DN[" + m_DNClient.getHost() + "]");
			}
			this.setCount(remoteCount);
			
		}
	}

	public void finalize() throws Throwable{
		this.close();
		super.finalize();
	}
	
	
	public void flush() throws IOException{
		m_dataOutputStream.flush();
	}
	
	public void writeTEItem(TEItem item) throws IOException {
		m_dataOutputStream.writeByte(0x01);
		item.write(m_dataOutputStream);
		this.localCount++;
	}

	/**
	 * Upload the partition ,which this writer is related to, from local repository
	 * engine to the Text Engine.
	 * 
	 * @param local_repository_engine
	 * @throws IOException
	 * @throws RepositoryEngineException
	 */
	public void writeLocalPartition(RepositoryEngine local_repository_engine, long itemCount) 
	throws IOException, RepositoryEngineException{
		PartitionEnumerator enu = local_repository_engine
				.getPartitionEnumerator(this.getYear(), this.getMonth(), this
						.getSiteID(), this.getForumID());
		long count = 0;
		while(enu.next() && count < itemCount){
			this.writeTEItem(enu.getItem());
			count ++;
		}
		enu.close();
		if (count != itemCount){
			throw new RepositoryEngineException("Can not read " + itemCount
					+ " items from local Partition.");
		}
	}

	public long getStartItemID() {
		return startItemID;
	}

	void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
	}

	public int getCount() {
		return count;
	}

	void setCount(int count) {
		this.count = count;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public String getSiteID() {
		return siteID;
	}

	public void setSiteID(String siteID) {
		this.siteID = siteID;
	}

	public String getForumID() {
		return forumID;
	}

	public void setForumID(String forumID) {
		this.forumID = forumID;
	}
}
