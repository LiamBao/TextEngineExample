package com.cic.textengine.repository.datanode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class PingResponse extends DNDaemonResponse{

	long upTime;
	

	public long getUpTime() {
		return upTime;
	}

	public void setUpTime(long upTime) {
		this.upTime = upTime;
	}

	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setUpTime(dis.readLong());
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeLong(this.getUpTime());
	}
	
}
