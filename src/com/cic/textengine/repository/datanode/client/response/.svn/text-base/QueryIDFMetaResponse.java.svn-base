package com.cic.textengine.repository.datanode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class QueryIDFMetaResponse extends DNDaemonResponse{
	
	int itemCount;
	boolean full;
	int maxItemCount;
	
	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setItemCount(dis.readInt());
		this.setFull(dis.readBoolean());
		this.setMaxItemCount(dis.readInt());
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(this.getItemCount());
		dos.writeBoolean(this.isFull());
		dos.writeInt(this.getMaxItemCount());
	}

	public int getItemCount() {
		return itemCount;
	}

	public void setItemCount(int itemCount) {
		this.itemCount = itemCount;
	}

	public boolean isFull() {
		return full;
	}

	public void setFull(boolean full) {
		this.full = full;
	}

	public int getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;
	}
}
