package com.cic.textengine.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.client.exception.TEItemEnumeratorException;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class TEItemEnumerator {
	private String nndaemonAddr = null;
	private int nndaemonPort = 0;

	RemoteTEItemEnumerator enu = null;

	public TEItemEnumerator(String host, int port, String partition_key,
			long start_item_id, int item_count, boolean includeDeleted)
			throws TEItemEnumeratorException {
		this.nndaemonAddr = host;
		this.nndaemonPort = port;
		NameNodeClient nn_client = new NameNodeClient(this.nndaemonAddr,
				this.nndaemonPort);
		PartitionKey key = null;
		try {
			key = PartitionKey.decodeStringKey(partition_key);
		} catch (DecoderException e) {
			throw new TEItemEnumeratorException(e);
		}
		DataNodeClient dn_client = null;
		try {
			dn_client = nn_client.getDNClientForQuery(key);
		} catch (NameNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		}
		try {
			enu = dn_client.getItemEnumerator(key.getYear(), key.getMonth(),
					key.getSiteID(), key.getForumID(), start_item_id,
					item_count, includeDeleted);
		} catch (DataNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		} catch (DataNodeClientCommunicationException e) {
			throw new TEItemEnumeratorException(e);
		}
	}

	public boolean next() throws TEItemEnumeratorException{
		try {
			return enu.next();
		} catch (IOException e) {
			throw new TEItemEnumeratorException(e);
		}
	}

	public TEItem getItem() {
		return enu.getItem();
	}

	public void close() {
		enu.close();
	}
	
	public TEItemEnumerator(String host, int port, String partition_key)
			throws TEItemEnumeratorException {
		this.nndaemonAddr = host;
		this.nndaemonPort = port;
		NameNodeClient nn_client = new NameNodeClient(this.nndaemonAddr,
				this.nndaemonPort);
		PartitionKey key = null;
		try {
			key = PartitionKey.decodeStringKey(partition_key);
		} catch (DecoderException e) {
			throw new TEItemEnumeratorException(e);
		}
		DataNodeClient dn_client = null;
		try {
			dn_client = nn_client.getDNClientForQuery(key);
		} catch (NameNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		}
		try {
			enu = dn_client.getItemEnumerator(key.getYear(), key.getMonth(),
					key.getSiteID(), key.getForumID(), 1,
					0, false);
		} catch (DataNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		} catch (DataNodeClientCommunicationException e) {
			throw new TEItemEnumeratorException(e);
		}
	}

	public TEItemEnumerator(String host, int port, String partition_key,long start_item_id)
			throws TEItemEnumeratorException {
		this.nndaemonAddr = host;
		this.nndaemonPort = port;
		NameNodeClient nn_client = new NameNodeClient(this.nndaemonAddr,
				this.nndaemonPort);
		PartitionKey key = null;
		try {
			key = PartitionKey.decodeStringKey(partition_key);
		} catch (DecoderException e) {
			throw new TEItemEnumeratorException(e);
		}
		DataNodeClient dn_client = null;
		try {
			dn_client = nn_client.getDNClientForQuery(key);
		} catch (NameNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		}
		try {
			enu = dn_client.getItemEnumerator(key.getYear(), key.getMonth(),
					key.getSiteID(), key.getForumID(), start_item_id,
					0, false);
		} catch (DataNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		} catch (DataNodeClientCommunicationException e) {
			throw new TEItemEnumeratorException(e);
		}
	}

	public TEItemEnumerator(String host, int port, String partition_key,long start_item_id,int item_count)
			throws TEItemEnumeratorException {
		this.nndaemonAddr = host;
		this.nndaemonPort = port;
		NameNodeClient nn_client = new NameNodeClient(this.nndaemonAddr,
				this.nndaemonPort);
		PartitionKey key = null;
		try {
			key = PartitionKey.decodeStringKey(partition_key);
		} catch (DecoderException e) {
			throw new TEItemEnumeratorException(e);
		}
		DataNodeClient dn_client = null;
		try {
			dn_client = nn_client.getDNClientForQuery(key);
		} catch (NameNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		}
		try {
			enu = dn_client.getItemEnumerator(key.getYear(), key.getMonth(),
					key.getSiteID(), key.getForumID(), start_item_id,
					item_count, false);
		} catch (DataNodeClientException e) {
			throw new TEItemEnumeratorException(e);
		} catch (DataNodeClientCommunicationException e) {
			throw new TEItemEnumeratorException(e);
		}
	}

}
