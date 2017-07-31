package com.cic.textengine.repository.namenode.daemon;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.broadcast.BroadcastPacket;
import com.cic.textengine.repository.broadcast.BroadcastPacketFactory;
import com.cic.textengine.repository.broadcast.DNKeyPacket;
import com.cic.textengine.repository.broadcast.exception.IllegalBroadcastPackageException;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistry;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.DataNodeIPAlreadyExistsException;
import com.cic.textengine.repository.namenode.manager.exception.IllegalNameNodeException;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.type.DNPartitionUpgradeVersion;

public class NNMulticastListenerThread extends Thread {
	Logger m_logger = Logger.getLogger(NNMulticastListenerThread.class);

	NNDaemon m_daemon = null;

	boolean m_stop = false;

	MulticastSocket m_multicastSocket = null;

	NNMulticastListenerThread(NNDaemon daemon) throws IOException {
		this.m_daemon = daemon;
		m_multicastSocket = new MulticastSocket(m_daemon.getMulticastPort());
		m_multicastSocket.joinGroup(m_daemon.getMulticastAddress());
	}

	public void start() {
		m_stop = false;
		super.start();
	}

	public void run() {
		byte[] buff = null;
		DatagramPacket datagramPacket = null;

		ExecutorService exec = Executors.newFixedThreadPool(50);

		while (!m_stop) {
			buff = new byte[1024];
			datagramPacket = new DatagramPacket(buff, buff.length);
			try {
				m_multicastSocket.receive(datagramPacket);
				exec.submit(new DatagramProcessor(datagramPacket));
			} catch (IOException e) {
				m_logger.error("Exception", e);
			}
		}
	}

	class DatagramProcessor implements Runnable {
		DatagramPacket m_pkt = null;

		public DatagramProcessor(DatagramPacket pkt) {
			m_pkt = pkt;
		}

		public void run() {
			boolean needSyncDN = false;
			DatagramPacket datagramPacket = m_pkt;
			ByteArrayInputStream bais = new ByteArrayInputStream(m_pkt
					.getData());

			BroadcastPacket packet;
			try {
				packet = BroadcastPacketFactory.retrieveBraodcastPackage(bais);
			} catch (IllegalBroadcastPackageException e) {
				m_logger.error("Exception", e);
				return;
			} catch (IOException e) {
				m_logger.error("Exception", e);
				return;
			}

			switch (packet.getPackageType()) {
			case BroadcastPacket.TYPE_BCPKG_DN_KEY:
				DNKeyPacket packet_instance = (DNKeyPacket) packet;
				DataNodeClient client = new DataNodeClient(datagramPacket
						.getAddress().getHostAddress(), packet_instance
						.getDNDaemonPort());

				// new data node, assign new key for the data node.
				String key = packet_instance.getKey();
				if ("".equals(key)) {
					try {
						key = NameNodeManagerFactory
								.getNameNodeManagerInstance().regsiterDataNode(
										datagramPacket.getAddress()
												.getHostAddress(),
										packet_instance.getDNDaemonPort());
						client.assignDataNodeKey(key);
					} catch (NameNodeManagerException e) {
						m_logger.error("Exception", e);
						break;
					} catch (DataNodeIPAlreadyExistsException e) {
						m_logger.error("Exception", e);
						break;
					} catch (DataNodeClientException e) {
						m_logger.error("Exception", e);
						break;
					} catch (DataNodeClientCommunicationException e) {
						m_logger.error("Exception", e);
						break;
					}
				}

				DNRegistry dnregistry = null;
				dnregistry = new DNRegistry();
				dnregistry.setDNKey(key);
				dnregistry
						.setHost(datagramPacket.getAddress().getHostAddress());
				dnregistry.setPort(packet_instance.getDNDaemonPort());
				dnregistry.setFreeSpace(packet_instance.getFreeSpace());

				if ("".equals(packet_instance.getNNDaemonAddress())
						|| DNRegistryTable.getInstance().getDNRegistry(key) == null) {
					m_logger
							.debug("Data node is found, assign the name node info to the data node.");
					// check the received data node key via the ip address
					try {
						NameNodeManagerFactory.getNameNodeManagerInstance()
								.activateNameNode(
										datagramPacket.getAddress()
												.getHostAddress(), key);
					} catch (NameNodeManagerException e) {
						m_logger.error("Exception", e);
						break;
					} catch (IllegalNameNodeException e) {
						m_logger.error("Exception", e);
						break;
					}

					// the data node is newly started
					try {
						client.assignNNDaemon("", m_daemon.getTcpPort());
					} catch (DataNodeClientException e) {
						m_logger.error("Exception", e);
						break;
					} catch (DataNodeClientCommunicationException e) {
						m_logger.error("Exception", e);
						break;
					}

					//count partition count on the data node
					try {
						dnregistry.setPartitionCount(NameNodeManagerFactory
								.getNameNodeManagerInstance()
								.getDNPartitionCount(key));
					} catch (NameNodeManagerException e) {
						m_logger.error(
								"Failed to get partiton count for data node.",
								e);
						break;
					}

					needSyncDN = true;
				}

				DNRegistryTable.getInstance().registerDN(dnregistry);

				// send sync task to the data node
				try {//sleep 1 min before send out all sync task. This can make sure all other data nodes are online.
					Thread.sleep(60000);
				} catch (InterruptedException e1) {
					//ignore
				}

				if (needSyncDN) {
					ArrayList<DNPartitionUpgradeVersion> version_list;
					try {
						version_list = NameNodeManagerFactory
								.getNameNodeManagerInstance()
								.listDNPartitionUpgradeVersion(key);
						for (DNPartitionUpgradeVersion dpuv : version_list) {
							try {
								client.syncPartition(dpuv.getYear(), dpuv
										.getMonth(), dpuv.getSiteid(), dpuv
										.getForumid(), dpuv.getVersion());
							} catch (DataNodeClientCommunicationException e) {
								m_logger.error(
												"Fail to send sync task to the data node.",
												e);
								break;
							} catch (DataNodeClientException e) {
								m_logger.error(
												"Fail to send sync task to the data node.",
												e);
								break;
							}
						}
					} catch (NameNodeManagerException e) {
						m_logger
								.error(
										"Failed to get Data node partition upgrade versions:",
										e);
						break;
					}
				}

				break;
			}
		}

	}
}
