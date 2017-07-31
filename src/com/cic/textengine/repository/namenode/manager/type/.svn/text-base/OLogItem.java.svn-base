package com.cic.textengine.repository.namenode.manager.type;

import java.io.IOException;

public abstract class OLogItem {
	int type;
	int partitionID;
	int version;
	
	
	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getPartitionID() {
		return partitionID;
	}

	public void setPartitionID(int partitionID) {
		this.partitionID = partitionID;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	public abstract byte[] getData() throws IOException;
}
