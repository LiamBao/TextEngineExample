package com.cic.textengine.repository.datanode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GetFreeSpaceResponse extends DNDaemonResponse{

	long space = 0;
	
	public long getSpace() {
		return space;
	}

	public void setSpace(long space) {
		this.space = space;
	}

	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setSpace(dis.readLong());		
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeLong(this.getSpace());
	}

}
