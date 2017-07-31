package com.cic.textengine.repository.datanode.client;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemEnumeratorException;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemWriterException;
import com.cic.textengine.repository.datanode.client.request.AssignKeyRequest;
import com.cic.textengine.repository.datanode.client.request.AssignNNDaemonRequest;
import com.cic.textengine.repository.datanode.client.request.CheckDNPartitionWriteLockRequest;
import com.cic.textengine.repository.datanode.client.request.DeleteItemsByConditionRequest;
import com.cic.textengine.repository.datanode.client.request.DeleteItemsRequest;
import com.cic.textengine.repository.datanode.client.request.GetFreeSpaceRequest;
import com.cic.textengine.repository.datanode.client.request.PingRequest;
import com.cic.textengine.repository.datanode.client.request.QueryItemsByConditionRequest;
import com.cic.textengine.repository.datanode.client.request.QueryItemsRequest;
import com.cic.textengine.repository.datanode.client.request.QueryOneItemRequest;
import com.cic.textengine.repository.datanode.client.request.SyncPartitionRequest;
import com.cic.textengine.repository.datanode.client.response.AssignKeyResponse;
import com.cic.textengine.repository.datanode.client.response.AssignNNDaemonResponse;
import com.cic.textengine.repository.datanode.client.response.CheckDNPartitionWriteLockResponse;
import com.cic.textengine.repository.datanode.client.response.DeleteItemsByConditionResponse;
import com.cic.textengine.repository.datanode.client.response.DeleteItemsResponse;
import com.cic.textengine.repository.datanode.client.response.GetFreeSpaceResponse;
import com.cic.textengine.repository.datanode.client.response.PingResponse;
import com.cic.textengine.repository.datanode.client.response.QueryItemsByConditionResponse;
import com.cic.textengine.repository.datanode.client.response.QueryItemsResponse;
import com.cic.textengine.repository.datanode.client.response.QueryOneItemResponse;
import com.cic.textengine.repository.datanode.client.response.SyncPartitionResponse;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.type.TEItem;

public class DataNodeClient {
	String host = null;
	int port = 0;
	NameNodeClient NNClient = null;
	String DNKey = null;
	
	private Socket daemon_socket= null;
	
	public DataNodeClient(String host, int port) {
		this.setHost(host);
		this.setPort(port);
	}
	
	
	/**
	 * Get the name node client where this data node client is retrieved from.
	 *  
	 * @return May return null if this data node client is not retrieved from the name node client.
	 */
	public NameNodeClient getNNClient() {
		return NNClient;
	}

	public void setNNClient(NameNodeClient client) {
		NNClient = client;
	}

	/**
	 * Assign an unique data node key to a new data node
	 * 
	 * @param key
	 */
	public void assignDataNodeKey(String key) throws DataNodeClientException,
			DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();
		try {
			AssignKeyRequest request = new AssignKeyRequest();

			request.setKey(key);
			request.write(socket.getOutputStream());

			AssignKeyResponse response = new AssignKeyResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this,response.getErrorMsg());
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

	public void syncPartition(int year, int month, String siteid,
			String forumid, int version)
			throws DataNodeClientCommunicationException,
			DataNodeClientException {
		Socket socket = connectDataNodeDaemon();
		try {
			SyncPartitionRequest request = new SyncPartitionRequest();
			request.setYear(year);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setForumID(forumid);
			request.setVersion(version);
			
			request.write(socket.getOutputStream());
			
			SyncPartitionResponse response = new SyncPartitionResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this,response.getErrorMsg());
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
	 * Assign the name node daemon to the data node. This method can only be
	 * invoked by a name node itself.
	 * 
	 * @param ip
	 * @param port
	 * @throws DataNodeClientException
	 */
	public void assignNNDaemon(String ip, int port)
			throws DataNodeClientException,
			DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();

		try {
			AssignNNDaemonRequest request = new AssignNNDaemonRequest();
			request.setNNDaemonIP(ip);
			request.setNNDaemonPort(port);

			request.write(socket.getOutputStream());

			AssignNNDaemonResponse response = new AssignNNDaemonResponse();
			response.read(socket.getInputStream());

			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this,response.getErrorMsg());
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

	public int checkDNPartitionWriteLock(int year, int month, String siteID,
			String forumID) throws DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();
		try {
			CheckDNPartitionWriteLockRequest request = new CheckDNPartitionWriteLockRequest();

			request.setYear(year);
			request.setMonth(month);
			request.setSiteID(siteID);
			request.setForumID(forumID);

			request.write(socket.getOutputStream());

			CheckDNPartitionWriteLockResponse response = new CheckDNPartitionWriteLockResponse();
			response.read(socket.getInputStream());
			return response.getOperation();
		} catch (IOException e) {
			throw new DataNodeClientCommunicationException(e);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
			}
		}
	}

	Socket connectDataNodeDaemon() 
	throws DataNodeClientCommunicationException {
		Socket socket;
		try {
			socket = new Socket(this.getHost(), this.getPort());
		} catch (UnknownHostException e) {
			if (this.getNNClient()!=null)//deactivate the data node if can not connect to it.
				this.getNNClient().deactivateDataNode(this.getDNKey());
			throw new DataNodeClientCommunicationException(e);
		} catch (IOException e) {
			if (this.getNNClient()!=null)//deactivate the data node if can not connect to it.
				this.getNNClient().deactivateDataNode(this.getDNKey());
			throw new DataNodeClientCommunicationException(e);
		}
		return socket;
	}

	/**
	 * Delete a series of items in a partition.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param itemid_list
	 * @throws DataNodeClientException
	 */
	public void deleteItems(int year, int month, String siteid, String forumid,
			ArrayList<Long> itemid_list, boolean sorted)
			throws DataNodeClientException,
			DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();
		try {
			DeleteItemsRequest request = new DeleteItemsRequest();

			request.setYear(year);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setForumID(forumid);
			request.setSorted(sorted);

			request.addItemIDList(itemid_list);

			request.write(socket.getOutputStream());

			DeleteItemsResponse response = new DeleteItemsResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this,response.getErrorMsg());
			}

			return;
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
	
	public ArrayList<TEItem> deleteItems(int year, int month, String siteid, String forumid, String condition) throws DataNodeClientCommunicationException, DataNodeClientException {
		Socket socket = connectDataNodeDaemon();
		
		try {
			DeleteItemsByConditionRequest request = new DeleteItemsByConditionRequest();
			request.setYear(year);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setForumID(forumid);
			request.setCondition(condition);
			request.write(socket.getOutputStream());
			
			DeleteItemsByConditionResponse response = new DeleteItemsByConditionResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this,response.getErrorMsg());
			}
			return response.getItemList();
		} catch(IOException e) {
			throw new DataNodeClientCommunicationException(e);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				// ignore here
			}
		}
	}
	
	public ArrayList<TEItem> queryItems(int year, int month, String siteid, String forumid, String condition) throws DataNodeClientCommunicationException, DataNodeClientException {
		Socket socket = connectDataNodeDaemon();
		
		QueryItemsByConditionRequest request = new QueryItemsByConditionRequest();
		request.setYear(year);
		request.setMonth(month);
		request.setSiteID(siteid);
		request.setForumID(forumid);
		request.setCondition(condition);
		try {
			request.write(socket.getOutputStream());
			
			QueryItemsByConditionResponse response = new QueryItemsByConditionResponse();
			response.read(socket.getInputStream());
			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this,response.getErrorMsg());
			}
			return response.getItemList();
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

	public String getHost() {
		return host;
	}

	/**
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param startItemID
	 * @param itemCount  	0 means return all items start from the startItemID.
	 * @param includeDeletedItems
	 * @return
	 * @throws DataNodeClientException
	 * @throws DataNodeClientCommunicationException
	 */
	public RemoteTEItemEnumerator getItemEnumerator(int year, int month,
			String siteid, String forumid, long startItemID, long itemCount, boolean includeDeletedItems) throws DataNodeClientException,
			DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();

		RemoteTEItemEnumerator enu = null;
		try {
			enu = new RemoteTEItemEnumerator(socket, year, month, siteid,
					forumid, startItemID,itemCount, includeDeletedItems);
		} catch (IOException e) {
			throw new DataNodeClientException(this,e);
		} catch (RemoteTEItemEnumeratorException e) {
			throw new DataNodeClientException(this,e);
		}
		return enu;
	}
	
	public RemoteTEItemEnumerator getItemEnumerator(int year, int month,
			String siteid, String forumid) throws DataNodeClientException,
			DataNodeClientCommunicationException {

		return getItemEnumerator(year, month, siteid, forumid, 1, 0, false);
	}

	public int getPort() {
		return port;
	}

	/**
	 * Get the write which can be used to stream TEItems to one of the datanode
	 * which contains the IDF.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 */
	public RemoteTEItemWriter getWriter(int year, int month, String siteid,
			String forumid) throws DataNodeClientException,
			DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();

		RemoteTEItemWriter writer;
		try {
			writer = new RemoteTEItemWriter(this, socket, year, month, siteid,
					forumid);
		} catch (IOException e) {
			throw new DataNodeClientException(this,e);
		} catch (RemoteTEItemWriterException e) {
			throw new DataNodeClientException(this,e);
		}
		return writer;
	}
	
	public long getFreeSpace() throws DataNodeClientCommunicationException, DataNodeClientException {
		Socket socket = connectDataNodeDaemon();
		
		try {
			 GetFreeSpaceRequest request = new GetFreeSpaceRequest();
			 request.write(socket.getOutputStream());
			 
			 GetFreeSpaceResponse response = new GetFreeSpaceResponse();
			 response.read(socket.getInputStream());
			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this, response.getErrorMsg());
			}
			 return response.getSpace();
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
	
	public DataNodeWriter getDNWriter() throws DataNodeClientCommunicationException, DataNodeClientException {
		Socket socket = connectDataNodeDaemon();
		DataNodeWriter writer;
		try {
			writer = new DataNodeWriter(this, socket);
		} catch (IOException e) {
			throw new DataNodeClientException(this, e);
		}
		return writer;
	}

	/**
	 * Ping the TextEngine data node daemon.
	 * 
	 * @return
	 * @throws DataNodeClientException
	 */
	public PingResponse ping() throws DataNodeClientException,
			DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();
		try {
			PingRequest request = new PingRequest();
			request.write(socket.getOutputStream());

			PingResponse pr = new PingResponse();
			pr.read(socket.getInputStream());
			return pr;
		} catch (IOException e) {
			throw new DataNodeClientException(this,e);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				// ignore here
			}
		}

	}

	public ArrayList<TEItem> queryItems(int year, int month, String siteid,
			String forumid, ArrayList<Long> itemid_list, boolean sorted)
			throws DataNodeClientException,
			DataNodeClientCommunicationException {
		Socket socket = connectDataNodeDaemon();
		try {
			QueryItemsRequest request = new QueryItemsRequest();

			request.setYear(year);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setForumID(forumid);

			request.addItemIDList(itemid_list);
			request.setSorted(sorted);

			request.write(socket.getOutputStream());

			QueryItemsResponse response = new QueryItemsResponse();
			response.read(socket.getInputStream());

			if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
				throw new DataNodeClientException(this,response.getErrorMsg());
			}
			return response.getTEItemList();
		} catch (IOException e) {
			throw new DataNodeClientException(this,e);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				// ignore here
			}
		}
	}
	
	public TEItem queryItem(int year, int month, String siteid, String forumid, long itemid)
	throws DataNodeClientCommunicationException, DataNodeClientException
	{
		if(daemon_socket == null)
		{
			daemon_socket = connectDataNodeDaemon();
			try {
				daemon_socket.setTcpNoDelay(true);
			} catch (SocketException e) {
				throw new DataNodeClientCommunicationException(e);
			}
		}
		
		TEItem item = null;
		
		try {
			BufferedOutputStream bos = new BufferedOutputStream(daemon_socket.getOutputStream());
			QueryOneItemRequest request = new QueryOneItemRequest();
			request.setYear(year);
			request.setMonth(month);
			request.setSiteID(siteid);
			request.setForumID(forumid);
			request.setItemID(itemid);
			request.write(bos);
			bos.flush();

			
			if (itemid != 0) {
				QueryOneItemResponse response = new QueryOneItemResponse();
				response.read(daemon_socket.getInputStream());
				if (response.getErrorCode() != DataNodeConst.ERROR_SUCCESS) {
					daemon_socket.close();
					daemon_socket = null;
					throw new DataNodeClientException(this, response
							.getErrorMsg());
				}
				item = response.getItem();
			} else {
				daemon_socket.close();
				daemon_socket = null;
			}
		} catch (IOException e) {
			throw new DataNodeClientException(this, e);
		}
		
		return item;
	}
	
	public TEItem queryItem(String itemkey) throws DecoderException, DataNodeClientCommunicationException, DataNodeClientException
	{
		ItemKey key = ItemKey.decodeKey(itemkey);
		TEItem item = queryItem(key.getYear(), key.getMonth(), key.getSource()+key.getSiteID(), key.getForumID(), key.getItemID());
		return item;
	}
	
	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}


	public String getDNKey() {
		return DNKey;
	}


	public void setDNKey(String key) {
		DNKey = key;
	}
	
	public void close()
	{
		if(daemon_socket != null)
		{
			try {
				this.queryItem(0, 0, "", "", 0);
			} catch (DataNodeClientCommunicationException e) {
				// ignore
			} catch (DataNodeClientException e) {
				// ignore
			} finally {
				try {
					daemon_socket.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}
	
}
