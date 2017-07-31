package com.cic.textengine.repository.namenode.daemon;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.daemon.exception.NNDaemonException;
import com.cic.textengine.repository.namenode.daemon.requesthandler.ApplyPartitionWriteLockRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.CleanNameNodeCacheRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.CleanPartitionRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.DeactivateDataNodeRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.GetDNAddressForAppendRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.GetDNAddressForQueryRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.GetDNClientForAppendRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.GetDNClientForQueryRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.GetDNListForQueryRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.GetDNPartitionItemCountRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.GetNextDNPartitionOperationRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.NNRequestContext;
import com.cic.textengine.repository.namenode.daemon.requesthandler.ReleasePartitionWriteLockRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.UpdateDNPartitionVersionRequestHandler;
import com.cic.textengine.repository.namenode.daemon.requesthandler.getDNPartitionAppendPointRequestHandler;
import com.cic.textengine.repository.partitionlock.PartitionWriteLockManager;

public class NNDaemon implements Runnable{
	static final String CONFIG_PROPERTIES_FILE = "NameNodeDaemon.properties";
	
	public void init(String args[]){
		
	}
	
	public static void main(String args[]){
		NNDaemon daemon;
		try {
			daemon = new NNDaemon();
		} catch (NNDaemonException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		daemon.start();
		try {
			daemon.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	PartitionWriteLockManager m_partitionWriteLockManager = null;
	
	Logger m_logger = Logger.getLogger(NNDaemon.class);
	int tcpPort = 0;
	
	boolean m_stop = false;
	int multicastPort = 0;
	
	InetAddress multicastAddress = null;
	
	Thread m_thread = null;
	
	long m_startTime = 0;
	

	int maxThreads = 20;
	int activeThreads = 0;
	
	NNMulticastListenerThread m_multicastListenerThread = null;
	NNPartitionLockCheckThread m_partitionLockCheckThread = null;
	
	public NNDaemon() throws NNDaemonException{
		m_partitionWriteLockManager = PartitionWriteLockManager.getInstance();
		loadConfig();

		m_logger.info("Initializing multicast sender thread.");
		try {
			m_multicastListenerThread = new NNMulticastListenerThread(this);
			m_partitionLockCheckThread = new NNPartitionLockCheckThread(this);
		} catch (IOException e) {
			throw new NNDaemonException(e);
		}
	}
	
	public int getMaxThreads(){
		return maxThreads;
	}
	
	void increaseActiveThreads(){
		activeThreads ++;
	}
	
	void decreaseActiveThreads(){
		activeThreads --;
	}
	
	

	public InetAddress getMulticastAddress() {
		return multicastAddress;
	}

	public int getMulticastPort() {
		return multicastPort;
	}
	
	public long getStartTime(){
		return m_startTime;
	}
	
	public int getTcpPort() {
		return tcpPort;
	}

	
	public void join() throws InterruptedException{
		m_thread.join();
	}
	
	void loadConfig() throws NNDaemonException{
		Properties props = new Properties();
		
		InputStream is = this.getClass().getResourceAsStream("/" + CONFIG_PROPERTIES_FILE);
		try {
			props.load(is);
			is.close();
		} catch (IOException e) {
			throw new NNDaemonException(e);
		}

		this.setTcpPort(Integer.parseInt(props.getProperty("port")));
		this.setMulticastPort(Integer.parseInt(props.getProperty("MultiCast.port")));
		try {
			this.setMulticastAddress(InetAddress.getByName(props.getProperty("MultiCast.address")));
		} catch (UnknownHostException e) {
			throw new NNDaemonException(e);
		}		
	}

	
	public void run() {
		
		m_logger.debug("CIC TextEngine datanode thread start running.");
		
		
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(tcpPort);
		} catch (IOException e) {
			m_logger.error("Failed to establish the server socket:", e);
			return;
		}
		
		m_logger.info("Start listening on server socket [port:" + this.getTcpPort() + "]");

		Socket socket = null;
		while(!m_stop){
			try {
				socket = serverSocket.accept();
				m_logger.debug("Get incoming connection socket,start serve the conneciton.");
				serveConnectedSocket(socket);
			} catch (IOException e) {
				m_logger.error("Error serving request:", e);
			} catch (NNDaemonException e) {
				m_logger.error("Error serving request:", e);
			}
		}
		try {
			serverSocket.close();
		} catch (IOException e) {
			m_logger.error("Error closing server socket:", e);
		}
	}


	void serveConnectedSocket(Socket socket)
	throws IOException,NNDaemonException{
		InputStream is = socket.getInputStream();
		OutputStream os = socket.getOutputStream();

		NNRequestContext requestContext = new NNRequestContext();
		requestContext.setDaemon(this);
		requestContext.setSocket(socket);
		
		byte[] buff = new byte[1];
		is.read(buff,0,1);
		
		NNRequestHandlerThread rh_thread = null;
		
		switch(buff[0]){
			case NameNodeConst.CMD_SHUTDOWN:	//shutdown the daemon.
				m_logger.debug("Get command for SHUTDOWN, shutting down DataNode daemon.");
				m_stop = true;
				break;
				
			case NameNodeConst.CMD_PING:
				m_logger.debug("Get command for PING.");
				break;
				
			case NameNodeConst.CMD_APPLY_PARTITION_WRITE_LOCK:
				m_logger.debug("Get command for APPLY_PARTITION_WRITE_LOCK.");
				rh_thread = new NNRequestHandlerThread(
					new ApplyPartitionWriteLockRequestHandler(),
					requestContext, socket, is, os);
				break;

			case NameNodeConst.CMD_RELEASE_PARTITION_WRITE_LOCK:
				m_logger.debug("Get command for RELEASE_PARTITION_WRITE_LOCK.");
				rh_thread = new NNRequestHandlerThread(
					new ReleasePartitionWriteLockRequestHandler(),
					requestContext,socket, is, os);
				break;

			case NameNodeConst.CMD_GET_DN_PARTITION_APPEND_POINT:
				m_logger.debug("Get command for CMD_GET_DN_PARTITION_APPEND_POINT.");
				rh_thread = new NNRequestHandlerThread(
					new getDNPartitionAppendPointRequestHandler(),
					requestContext,socket, is, os);
				break;

			case NameNodeConst.CMD_GET_DN_CLIENT_FOR_WRITING:
				m_logger.debug("Get command for CMD_GET_DN_CLIENT_FOR_APPENDING.");
				rh_thread = new NNRequestHandlerThread(
					new GetDNClientForAppendRequestHandler(),
					requestContext,socket, is, os);
				break;
				
			case NameNodeConst.CMD_GET_DN_CLIENT_FOR_QUERY:
				m_logger.debug("Get command for CMD_GET_DN_CLIENT_FOR_QUERY.");
				rh_thread = new NNRequestHandlerThread(
					new GetDNClientForQueryRequestHandler(),
					requestContext,socket, is, os);
				break;

			case NameNodeConst.CMD_GET_DN_PARTITION_ITEM_COUNT:
				m_logger.debug("Get command for CMD_GET_DN_PARTITION_ITEM_COUNT.");
				rh_thread = new NNRequestHandlerThread(
					new GetDNPartitionItemCountRequestHandler(),
					requestContext,socket, is, os);
				break;

			case NameNodeConst.CMD_CLEAN_PARTITION:
				m_logger.debug("Get command for CMD_CLEAN_PARTITION.");
				rh_thread = new NNRequestHandlerThread(
					new CleanPartitionRequestHandler(),
					requestContext,socket, is, os);
				break;

			case NameNodeConst.CMD_GET_NEXT_DN_PARTITION_OPERATION:
				m_logger.debug("Get command for CMD_GET_NEXT_DN_PARTITION_OPERATION.");
				rh_thread = new NNRequestHandlerThread(
					new GetNextDNPartitionOperationRequestHandler(),
					requestContext,socket, is, os);
				break;

			case NameNodeConst.CMD_UPDATE_DN_PARTITION_VERSION:
				m_logger.debug("Get command for CMD_UPDATE_DN_PARTITION_VERSION.");
				rh_thread = new NNRequestHandlerThread(
					new UpdateDNPartitionVersionRequestHandler(),
					requestContext,socket, is, os);
				break;

			case NameNodeConst.CMD_DEACTIVATE_DATA_NODE:
				m_logger.debug("Get command for CMD_DEACTIVATE_DATA_NODE.");
				rh_thread = new NNRequestHandlerThread(
					new DeactivateDataNodeRequestHandler(),
					requestContext,socket, is, os);
				break;
			
			case NameNodeConst.CMD_GET_DN_ADDRESS_FOR_QUERY:
				m_logger.debug("Get command for CMD_GET_DN_ADDRESS_FOR_QUERY");
				rh_thread = new NNRequestHandlerThread(
					new GetDNAddressForQueryRequestHandler(), 
					requestContext, socket, is, os);
				break;
			case NameNodeConst.CMD_GET_DN_ADDRESS_FOR_APPEND:
				m_logger.debug("Get command for CMD_GET_DN_ADDRESS_FOR_APPEND");
				rh_thread = new NNRequestHandlerThread(
					new GetDNAddressForAppendRequestHandler(), requestContext,
					socket, is, os);
				break;
			case NameNodeConst.CMD_CLEAN_NN_CACHE:
				m_logger.debug("Get command for CMD_CLEAN_NN_CACHE");
				rh_thread = new NNRequestHandlerThread(
					new CleanNameNodeCacheRequestHandler(), requestContext,
					socket, is, os);
				break;
			case NameNodeConst.CMD_GET_DN_LIST_FOR_QUERY:
				m_logger.debug("Get comment for CMD_GET_DN_LIST_FOR_QUERY");
				rh_thread = new NNRequestHandlerThread(
						new GetDNListForQueryRequestHandler(), requestContext,
						socket, is, os);
				break;
		}
		//start the process thread.
		if (rh_thread != null)
			rh_thread.start();
	}


	public void setMulticastAddress(InetAddress multicastAddress) {
		this.multicastAddress = multicastAddress;
	}


	public void setMulticastPort(int multicastPort) {
		this.multicastPort = multicastPort;
	}


	public void setTcpPort(int tcpPort) {
		this.tcpPort = tcpPort;
	}

	public synchronized void start(){
		m_logger.info("Start CIC TextEngine Datanode Daemon.");
		m_startTime = System.currentTimeMillis();
		m_stop = false;
		m_thread = new Thread(this);
		//m_thread.setDaemon(true);
		m_thread.start();

		m_multicastListenerThread.start();
		m_partitionLockCheckThread.start();
	}
	
	/**
	 * Stop the daemon service.
	 */
	public synchronized void stop(){
		m_stop = true;
		Socket socket;
		try {
			socket = new Socket("127.0.0.1", this.getTcpPort());
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.writeByte(NameNodeConst.CMD_SHUTDOWN);
			dos.close();
		} catch (UnknownHostException e) {
			m_logger.error("Exception when shutting down name node daemon.", e);
		} catch (IOException e) {
			m_logger.error("Exception when shutting down name node daemon.", e);
		}
		return;
	}
	
	public synchronized void destroy(){
		
	}
}
