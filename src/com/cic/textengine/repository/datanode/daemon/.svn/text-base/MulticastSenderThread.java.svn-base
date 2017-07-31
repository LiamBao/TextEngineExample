package com.cic.textengine.repository.datanode.daemon;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.broadcast.DNKeyPacket;

/**
 * This thread ongoing broadcast all the IDF that it contains.
 * @author denis.yu
 *
 */
public class MulticastSenderThread implements Runnable{
	static final String CONFIG_PROPERTIES_FILE = "DataNodeDaemon.properties";

	Logger m_logger = Logger.getLogger(MulticastSenderThread.class);

	int m_heartBeat = 10000;
	
	DNDaemon m_daemon = null;
	
	boolean m_stop = false;
	
	MulticastSocket m_multicastSocket = null;
	
	Thread m_thread = null;
	
	MulticastSenderThread(DNDaemon daemon)
	throws IOException{
		loadConfig();
		
		m_daemon = daemon;
		
		m_multicastSocket = new MulticastSocket();
		m_multicastSocket.setLoopbackMode(false);
	}

	
	void loadConfig() throws IOException{
		Properties props = new Properties();
		
		InputStream is = this.getClass().getResourceAsStream("/" + CONFIG_PROPERTIES_FILE);
		props.load(is);
		is.close();

		m_heartBeat = Integer.parseInt(props.getProperty("MultiCast.Sender.HEARTBEAT.MS"));
	}
	
	public void start(){
		m_thread = new Thread(this);
		m_thread.start();
	}
	
	public synchronized void stop(){
		m_stop = true;
	}
	
	public void run(){
		while(!m_stop){
			DatagramPacket packet = null;
			byte[] buff = null;
			
			String key = m_daemon.getDataNodeKey();
			if (key == null){
				key = "";
			}
			
			File repo = new File(this.m_daemon.m_dataNodeRepositoryFolder);
			long freeSpace = repo.getUsableSpace();
			
			DNKeyPacket pkt = new DNKeyPacket();
			pkt.setKey(key);
			pkt.setDNDaemonPort(m_daemon.getTcpPort());
			pkt.setNNDaemonAddress(m_daemon.getNNDaemonIP() == null?"":m_daemon.getNNDaemonIP());
			pkt.setNNDamonPort(m_daemon.getNNDaemonPort());
			pkt.setFreeSpace(freeSpace);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			try {
				pkt.write(dos);
			} catch (IOException e2) {
				m_logger.error("Error preparing the broadcast package.", e2);
			}
			buff = baos.toByteArray();
			
			
			
			packet = new DatagramPacket(buff, buff.length , m_daemon.getMulticastAddress(), m_daemon.getMulticastPort());
			try {
				m_multicastSocket.send(packet);
			} catch (IOException e1) {
				m_logger.error("Error sending out broadcast messages to [addr:" + 
						m_daemon.getMulticastAddress() + ",port:" +
						m_daemon.getMulticastPort() +
						"]",e1);
			}
			
			try {
				Thread.sleep(m_heartBeat);
			} catch (InterruptedException e) {
				//ignore
			}
		}
	}
}
