package com.cic.textengine.repository.namenode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ApplyPartitionWriteLockResponse extends NNDaemonResponse {
	long startItemID = -1;
	int targetVersion = 0;
	
	public int getTargetVersion() {
		return targetVersion;
	}

	public void setTargetVersion(int targetVersion) {
		this.targetVersion = targetVersion;
	}

	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setStartItemID(dis.readLong());
		this.setTargetVersion(dis.readInt());
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeLong(this.getStartItemID());
		dos.writeInt(this.getTargetVersion());
	}

	public long getStartItemID() {
		return startItemID;
	}

	public void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
	}

}
