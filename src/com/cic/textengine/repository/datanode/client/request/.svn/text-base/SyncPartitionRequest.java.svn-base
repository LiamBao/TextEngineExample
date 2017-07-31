package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.datanode.DataNodeConst;


public class SyncPartitionRequest extends DNPartitionRequest {
	int version;

	public SyncPartitionRequest(){
		this.setType(DataNodeConst.CMD_SYNC_PARTITION);
	}
	

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}


	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setVersion(dis.readInt());
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeInt(this.getVersion());
	}
}
