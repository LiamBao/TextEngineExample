package com.cic.textengine.repository.namenode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.namenode.NameNodeConst;

public class UpdateDNPartitionVersionRequest extends NNPartitionRequest{
	String DNKey;
	int version = 0;
	long itemCount = 0;
	
	public long getItemCount() {
		return itemCount;
	}

	public void setItemCount(long itemCount) {
		this.itemCount = itemCount;
	}

	public UpdateDNPartitionVersionRequest(){
		this.setType(NameNodeConst.CMD_UPDATE_DN_PARTITION_VERSION);
	}
	
	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getDNKey() {
		return DNKey;
	}

	public void setDNKey(String key) {
		DNKey = key;
	}


	

	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setVersion(dis.readInt());
		this.setDNKey(dis.readUTF());
		this.setItemCount(dis.readLong());
	}
	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeInt(this.getVersion());
		dos.writeUTF(this.getDNKey());
		dos.writeLong(this.getItemCount());
	}
}
