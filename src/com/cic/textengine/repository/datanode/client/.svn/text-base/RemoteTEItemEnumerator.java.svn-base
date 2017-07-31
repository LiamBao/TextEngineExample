package com.cic.textengine.repository.datanode.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.TEItemInputStream;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemEnumeratorException;
import com.cic.textengine.repository.datanode.client.request.EnumerateItemRequest;
import com.cic.textengine.repository.datanode.client.response.EnumerateItemResponse;
import com.cic.textengine.type.TEItem;

public class RemoteTEItemEnumerator {
	Socket m_socket = null;
	OutputStream m_outputStream = null;
	DataInputStream m_dataInputStream = null;
	TEItemInputStream m_teitemInputStream = null;
	InputStream m_inputStream = null;
	TEItem m_item = null;

	long itemCount = 0;
	long currentItemID = 0;
	
	RemoteTEItemEnumerator(Socket socket, int year, int month,
			String siteid, String forumid, long startItemID, long itemCount, boolean includeDeletedItems) 
	throws IOException, RemoteTEItemEnumeratorException {
		m_socket = socket;
		m_outputStream = socket.getOutputStream();
		m_inputStream = socket.getInputStream();
		m_dataInputStream = new DataInputStream(m_inputStream);
		m_teitemInputStream = new TEItemInputStream(m_inputStream);
		
		EnumerateItemRequest request = new EnumerateItemRequest();
		request.setForumID(forumid);
		request.setSiteID(siteid);
		request.setYear(year);
		request.setMonth(month);
		request.setStartItemID(startItemID);
		request.setIncludeDeletedItems(includeDeletedItems);
		request.setItemCount(itemCount);
		request.write(m_outputStream);
		
		
		EnumerateItemResponse response = new EnumerateItemResponse();
		response.read(m_inputStream);
		if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS){
			throw new RemoteTEItemEnumeratorException(response.getErrorMsg());
		}		
		
		this.itemCount = response.getItemCount();
		this.currentItemID = 1;
	}

	/**
	 * Move to next item.
	 * @return	False if there's no next item. True if there's a next item. 
	 * 			Call getItem to get the item object.
	 * @throws IOException
	 */
	public boolean next() throws IOException{
		if (this.currentItemID > this.itemCount){
			return false;
		}
		
		byte b = m_dataInputStream.readByte();
		
		if (b == 0x00){
			m_item = null;
			return false;
		}else{
			m_item = m_teitemInputStream.readItem();
			this.currentItemID++;
			return true;
		}
	}
	
	/**
	 * Retrieve the current item in the enumerator.
	 * 
	 * @return
	 */
	public TEItem getItem(){
		return m_item;
	}
	
	/**
	 * Close the enumerator. 
	 */
	public void close(){
		try {
			if (m_inputStream != null){
				m_inputStream.close();
				m_inputStream = null;
			}
			if (m_outputStream != null){
				m_outputStream.close();
				m_outputStream =  null;
			}
			if (m_socket != null){
				m_socket.close();
				m_socket = null;
			}
		} catch (IOException e) {
			//ignore
		}
	}
	
	public void finalize() throws Throwable{
		this.close();
		super.finalize();
	}
}
