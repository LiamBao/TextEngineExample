package com.cic.textengine.repository.namenode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.namenode.NameNodeConst;

public class ReleasePartitionWriteLockRequest extends NNPartitionRequest{

	String DNKey;
	//0: means the operation is failed, then release the lock
	//1: append, 2:clean, 3: delete
	int operation;
	
	//the following properties are used by append aperation
	long startItemID;
	int itemCount;
	
	//the following properties are used by delete operation.
	int targetVersion;

	public ReleasePartitionWriteLockRequest(){
		this.setType(NameNodeConst.CMD_RELEASE_PARTITION_WRITE_LOCK);
	}

	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setDNKey(dis.readUTF());
		this.setStartItemID(dis.readLong());
		this.setItemCount(dis.readInt());
		this.setOperation(dis.readInt());
		this.setTargetVersion(dis.readInt());
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeUTF(this.getDNKey());
		dos.writeLong(this.getStartItemID());
		dos.writeInt(this.getItemCount());
		dos.writeInt(this.getOperation());
		dos.writeInt(this.getTargetVersion());
	}



	public String getDNKey() {
		return DNKey;
	}

	public void setDNKey(String key) {
		DNKey = key;
	}

	public long getStartItemID() {
		return startItemID;
	}

	public void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
	}

	public int getItemCount() {
		return itemCount;
	}

	public void setItemCount(int itemCount) {
		this.itemCount = itemCount;
	}

	public int getOperation() {
		return operation;
	}

	public void setOperation(int operation) {
		this.operation = operation;
	}

	public int getTargetVersion() {
		return targetVersion;
	}

	public void setTargetVersion(int targetVersion) {
		this.targetVersion = targetVersion;
	}

}
