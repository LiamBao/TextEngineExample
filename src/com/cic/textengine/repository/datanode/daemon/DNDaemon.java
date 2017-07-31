package com.cic.textengine.repository.datanode.daemon;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.daemon.exception.DNDaemonException;
import com.cic.textengine.repository.datanode.daemon.exception.DataNodeKeyAlreadyExistsException;
import com.cic.textengine.repository.datanode.daemon.requesthandler.AddItemsByStreamHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.AddItemsRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.AssignDNKeyRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.AssignNNDaemonRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.CheckDNPartitionWriteLockRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.DNRequestContext;
import com.cic.textengine.repository.datanode.daemon.requesthandler.DeleteItemsByConditionRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.DeleteItemsRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.EnumerateItemRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.GetFreeSpaceRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.PersistAddItemsRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.PingRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.QueryItemsByConditionRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.QueryItemsRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.QueryOneItemRequestHandler;
import com.cic.textengine.repository.datanode.daemon.requesthandler.SyncPartitionRequestHandler;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;

/**
 * Daemon which acts as a server to serve client requests
 * @author denis.yu
 *
 */
public class DNDaemon implements Runnable{
	static final String CONFIG_PROPERTIES_FILE = "DataNodeDaemon.properties";
	static final String DATA_NODE_KEY_FILE = "DataNode.key";
		
	public static void main(String args[]){
		DNDaemon daemon;
		try {
			daemon = new DNDaemon();
		} catch (DNDaemonException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		try {
			daemon.start();
		} catch (DNDaemonException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			daemon.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	Logger m_logger = Logger.getLogger(DNDaemon.class);
	
	public String m_dataNodeRepositoryFolder = null;
	int tcpPort = 0;
	
	boolean m_stop = false;
	int multicastPort = 0;
	
	InetAddress multicastAddress = null;
	
	Thread m_thread = null;
	
	long m_startTime = 0;
	
	String dataNodeKey = null;
	
	RepositoryEngine m_repoEngine = null;
	MulticastSenderThread m_multicastSenderThread = null;
	
	DNSynchronizer m_DNSynchronizer = null;
	String NNDaemonIP = null;
	
	int NNDaemonPort = 0;
	int maxThreads = 20;
	
	int activeThreads = 0;
	
	public DNDaemon() throws DNDaemonException{
		loadConfig();

		loadKey();
		
		
		m_logger.info("Initializing local repository engine...");
		try {
			m_repoEngine = RepositoryFactory.getNewRepositoryEngineInstance(m_dataNodeRepositoryFolder);
		} catch (RepositoryEngineException e1) {
			throw new DNDaemonException(e1);
		}

		m_logger.info("Initializing multicast sender thread.");
		try {
			m_multicastSenderThread = new MulticastSenderThread(this);
		} catch (IOException e) {
			throw new DNDaemonException(e);
		}
		
		m_DNSynchronizer = new DNSynchronizer(this);
		
	}

	void decreaseActiveThreads(){
		activeThreads --;
	}
	
	public synchronized void destroy(){
		
	}
	
	public String getDataNodeKey() {
		return dataNodeKey;
	}
		
	public String getDataNodeRepositoryFolder() {
		return m_dataNodeRepositoryFolder;
	}
	
	public DNSynchronizer getDNSynchronizer(){
		return m_DNSynchronizer;
	}
	
	
	public int getMaxThreads(){
		return maxThreads;
	}

	public InetAddress getMulticastAddress() {
		return multicastAddress;
	}
	
	public int getMulticastPort() {
		return multicastPort;
	}
	
	public String getNNDaemonIP() {
		return NNDaemonIP;
	}
	
	public int getNNDaemonPort() {
		return NNDaemonPort;
	}
	
	public RepositoryEngine getRepositoryEngine(){
		return m_repoEngine;
	}
	
	public long getStartTime(){
		return m_startTime;
	}

	
	public int getTcpPort() {
		return tcpPort;
	}


	void increaseActiveThreads(){
		activeThreads ++;
	}


	public void join() throws InterruptedException{
		m_thread.join();
	}


	void loadConfig() throws DNDaemonException{
		Properties props = new Properties();
		
		InputStream is = this.getClass().getResourceAsStream("/" + CONFIG_PROPERTIES_FILE);
		try {
			props.load(is);
			is.close();
		} catch (IOException e) {
			throw new DNDaemonException(e);
		}

		this.setTcpPort(Integer.parseInt(props.getProperty("port")));
		this.setDataNodeRepositoryFolder(props.getProperty("IDFRepository.folder"));
		this.setMulticastPort(Integer.parseInt(props.getProperty("MultiCast.port")));
		
		try {
			this.setMulticastAddress(InetAddress.getByName(props.getProperty("MultiCast.address")));
		} catch (UnknownHostException e) {
			throw new DNDaemonException(e);
		}		
	}


	void loadKey() throws DNDaemonException{
		String key = null;
		
		
		m_logger.info("Try to load the data node key...");
		File file = new File(this.getDataNodeRepositoryFolder()
				+ File.separator + DATA_NODE_KEY_FILE);
		if (!(file.isFile() && file.exists())){
			
			m_logger.info("Can not file data node key, this is supposed to be a new data node.");
			key = null;
			return;
		}
		try {
			LineNumberReader lnr = new LineNumberReader( new FileReader(file));
			key = lnr.readLine();
		} catch (IOException e) {
			throw new DNDaemonException(e);
		}
		
		//valid the key throuhg UUID validator
		try{
			UUID.fromString(key);
			m_logger.info("Data node key found:" + key);
		}catch(Exception e){
			throw new DNDaemonException(e);
		}
		
		dataNodeKey = key;
	}

	public void run() {
		
		m_logger.debug("CIC TextEngine datanode thread start running.");
		
		ServerSocket serverSocket = null;
		try {
			m_logger.info("Initiating server socket on [port:" + this.getTcpPort() + "]");
			serverSocket = new ServerSocket(this.getTcpPort());
		} catch (IOException e) {
			m_logger.error("Failed to establish the server socket:", e);
		}		
		m_logger.info("Server socket is established.");
		
		Socket socket = null;
		m_logger.debug("Start waiting for income socket connection");
		while(!m_stop){
			try {
				socket = serverSocket.accept();
				m_logger.debug("Get incoming connection socket,start serving the connection.");
				serveConnectedSocket(socket);
			} catch (IOException e) {
				m_logger.error("Error serving request:", e);
			} catch (DNDaemonException e) {
				m_logger.error("Error serving request:", e);
			}
		}
		try {
			serverSocket.close();
		} catch (IOException e) {
			m_logger.error("Error closing server socket:", e);
		}
		m_logger.info("DNDaemon is stopped.");
	}

	void serveConnectedSocket(Socket socket)
	throws IOException,DNDaemonException{
		InputStream is = socket.getInputStream();
		OutputStream os = socket.getOutputStream();
		byte[] buff = new byte[1];
		is.read(buff,0,1);

		DNRequestContext requestContext = new DNRequestContext();
		requestContext.setDaemon(this);
		requestContext.setSocket(socket);

		DNRequestHandlerThread rh_thread = null;
		
		switch(buff[0]){
			case DataNodeConst.CMD_SHUTDOWN:	//shutdown the daemon.
				m_logger.debug("Get command for SHUTDOWN, shutting down DataNode daemon.");
				m_stop = true;
				break;
				
			case DataNodeConst.CMD_PING:
				m_logger.debug("Get command for PING.");
				rh_thread = new DNRequestHandlerThread(
						new PingRequestHandler(),
						requestContext, socket, is, os);
				break;
			case DataNodeConst.CMD_ADD_ITEMS:
				m_logger.debug("Get command for CMD_ADD_ITEMS.");
				rh_thread = new DNRequestHandlerThread(
					new AddItemsRequestHandler(),
					requestContext, socket, is, os);
				break;
			case DataNodeConst.CMD_ADD_ITEMS_STREAM:
				m_logger.debug("Get command for ADD ITEMS (STREAM MODE).");
				rh_thread = new DNRequestHandlerThread(
					new AddItemsByStreamHandler(),
					requestContext, socket, is, os);
				break;
			case DataNodeConst.CMD_QUERY_ITEMS:
				m_logger.debug("Get command for CMD_QUERY_ITEMS.");
				rh_thread = new DNRequestHandlerThread(
					new QueryItemsRequestHandler(),
					requestContext, socket, is, os);
				break;
			case DataNodeConst.CMD_REMOVE_ITEMS:
				m_logger.debug("Get command for CMD_REMOVE_ITEMS.");
				rh_thread = new DNRequestHandlerThread(
						new DeleteItemsRequestHandler(),
						requestContext, socket, is, os);
				break;
			case DataNodeConst.CMD_QUERY_IDF_META:
				m_logger.debug("Get command for CMD_QUERY_IDF_META");
				//TODO: Add process here
				break;
			case DataNodeConst.CMD_ASSIGN_DN_KEY:
				m_logger.debug("Get command for CMD_ASSIGN_DN_KEY");
				rh_thread = new DNRequestHandlerThread(
						new AssignDNKeyRequestHandler(),
						requestContext, socket, is, os);
				
				break;
			case DataNodeConst.CMD_ASSIGN_NN_DAEMON:
				m_logger.debug("Get command for CMD_ASSIGN_NN_DAEMON");
				rh_thread = new DNRequestHandlerThread(
					new AssignNNDaemonRequestHandler(),
					requestContext, socket, is, os);
				
				break;

			case DataNodeConst.CMD_ENUMERATE_ITEM:
				m_logger.debug("Get command for CMD_ENUMERATE_ITEM");
				rh_thread = new DNRequestHandlerThread(
					new EnumerateItemRequestHandler(),
					requestContext, socket, is, os);
				
				break;

			case DataNodeConst.CMD_CHECK_DN_PARTITION_WRITE_LOCK:
				m_logger.debug("Get command for CMD_CHECK_DN_PARTITION_WRITE_LOCK");
				rh_thread = new DNRequestHandlerThread(
					new CheckDNPartitionWriteLockRequestHandler(),
					requestContext, socket, is, os);
				
				break;
			case DataNodeConst.CMD_SYNC_PARTITION:
				m_logger.debug("Get command for CMD_SYNC_PARTITION");
				rh_thread = new DNRequestHandlerThread(
					new SyncPartitionRequestHandler(),
					requestContext, socket, is, os);
				
				break;
			case DataNodeConst.CMD_QUERY_ONE_ITEM:
				m_logger.debug("Get command for CMD_QUERY_ONE_ITEM");
				rh_thread = new DNRequestHandlerThread(
						new QueryOneItemRequestHandler(),
						requestContext, socket, is, os);
				break;
			case DataNodeConst.CMD_PERSIST_ADD_ITEMS:
				m_logger.debug("Get command for CMD_PERSIST_ADD_ITEMS");
				rh_thread = new DNRequestHandlerThread(
						new PersistAddItemsRequestHandler(),
						requestContext, socket, is, os);
				break;
			case DataNodeConst.CMD_DELETE_ITEMS_BYCONDITION:
				m_logger.debug("Get command for CMD_DELETE_ITEMS_BYCONDITION");
				rh_thread = new DNRequestHandlerThread(
					new DeleteItemsByConditionRequestHandler(), requestContext,
					socket, is, os);
				break;
			case DataNodeConst.CMD_GET_FREE_SPACE:
				m_logger.debug("Get command for CMD_GET_FREE_SPACE");
				rh_thread = new DNRequestHandlerThread(
					new GetFreeSpaceRequestHandler(), requestContext, socket,
					is, os);
				break;
			case DataNodeConst.CMD_QUERY_ITEMS_BYCONDITION:
				m_logger.debug("Get command for CMD_QUERY_ITEMS_BYCONDITION");
				rh_thread = new DNRequestHandlerThread(
					new QueryItemsByConditionRequestHandler(), requestContext, socket,
					is, os);
				break;
		}
		
		//start the process thread.
		if (rh_thread!=null)
			rh_thread.start();
	}

	/**
	 * Set the key for the data node. The key will be persistented in the data node. 
	 * 
	 * @param dataNodeKey
	 * @throws DNDaemonException
	 * @throws DataNodeKeyAlreadyExistsException
	 */
	public void setDataNodeKey(String dataNodeKey) throws DNDaemonException,
			DataNodeKeyAlreadyExistsException {
		if (this.getDataNodeKey()!=null){
			throw new DataNodeKeyAlreadyExistsException();
		}
		//save key
		
		
		File file = new File(this.getDataNodeRepositoryFolder()
				+ File.separator + DATA_NODE_KEY_FILE);

		FileWriter fw;
		try {
			fw = new FileWriter(file,false);
			fw.write(dataNodeKey);
			fw.close();
		} catch (IOException e) {
			throw new DNDaemonException(e);
		}

		this.dataNodeKey = dataNodeKey;
	}

	public void setDataNodeRepositoryFolder(String dataNodeRepositoryFolder) {
		this.m_dataNodeRepositoryFolder = dataNodeRepositoryFolder;
	}

	public void setMulticastAddress(InetAddress multicastAddress) {
		this.multicastAddress = multicastAddress;
	}

	
	public void setMulticastPort(int multicastPort) {
		this.multicastPort = multicastPort;
	}

	public void setNNDaemonIP(String daemonIP) {
		NNDaemonIP = daemonIP;
	}

	public void setNNDaemonPort(int daemonPort) {
		NNDaemonPort = daemonPort;
	}

	public void setTcpPort(int tcpPort) {
		this.tcpPort = tcpPort;
	}

	public synchronized void start() throws DNDaemonException{
		m_logger.info("Start CIC TextEngine Datanode Daemon.");


		m_startTime = System.currentTimeMillis();

		m_multicastSenderThread.start();
		m_DNSynchronizer.start();

		m_stop = false;
		m_thread = new Thread(this);
		//m_thread.setDaemon(true);
		m_thread.start();
		
	}

	public synchronized void stop(){
		m_stop = true;

		m_multicastSenderThread.stop();

		m_stop = true;
		Socket socket;
		try {
			socket = new Socket("127.0.0.1", this.getTcpPort());
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.writeByte(DataNodeConst.CMD_SHUTDOWN);
			dos.close();
		} catch (UnknownHostException e) {
			m_logger.error("Exception when shutting down name node daemon.", e);
		} catch (IOException e) {
			m_logger.error("Exception when shutting down name node daemon.", e);
		}
		return;
	}

	public void init(String args[]){
		
	}

}
