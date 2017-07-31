package com.cic.textengine.repository.namenode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GetDNClientForWritingResponse extends NNDaemonResponse{
	String host;
	int port;
	String DNKey;
	
	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setHost(dis.readUTF());
		this.setPort(dis.readInt());
		this.setDNKey(dis.readUTF());
		
	}
	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeUTF(this.getHost());
		dos.writeInt(this.getPort());
		dos.writeUTF(this.getDNKey());
		
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
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
}
