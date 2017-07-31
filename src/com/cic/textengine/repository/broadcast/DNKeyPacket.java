package com.cic.textengine.repository.broadcast;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DNKeyPacket extends BroadcastPacket {
	String key = "";
	int DNDaemonPort = 0;
	String NNDaemonAddress = "";
	int NNDamonPort = 0;
	long freeSpace = 0;
	
	public long getFreeSpace() {
		return freeSpace;
	}

	public void setFreeSpace(long freeSpace) {
		this.freeSpace = freeSpace;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key == null?"":key;
	}

	public DNKeyPacket(){
		this.setPackageType(BroadcastPacket.TYPE_BCPKG_DN_KEY);
	}

	public void readBody(InputStream is)  throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setKey(dis.readUTF());
		this.setDNDaemonPort(dis.readInt());
		this.setNNDaemonAddress(dis.readUTF());
		this.setNNDamonPort(dis.readInt());
		this.setFreeSpace(dis.readLong());
	}

	@Override
	public void writeBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeUTF(this.getKey());
		dos.writeInt(this.getDNDaemonPort());
		dos.writeUTF(this.getNNDaemonAddress());
		dos.writeInt(this.getNNDamonPort());
		dos.writeLong(this.getFreeSpace());
	}

	public int getDNDaemonPort() {
		return DNDaemonPort;
	}

	public void setDNDaemonPort(int daemonPort) {
		DNDaemonPort = daemonPort;
	}

	public String getNNDaemonAddress() {
		return NNDaemonAddress;
	}

	public void setNNDaemonAddress(String daemonAddress) {
		NNDaemonAddress = daemonAddress;
	}

	public int getNNDamonPort() {
		return NNDamonPort;
	}

	public void setNNDamonPort(int damonPort) {
		NNDamonPort = damonPort;
	}
}
