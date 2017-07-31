package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class PersistAddItemsRequest extends DNPartitionRequest{

	long startItemID;
	
	public PersistAddItemsRequest(){
		this.setType(DataNodeConst.CMD_PERSIST_ADD_ITEMS);
	}
	
	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setStartItemID(dis.readLong());
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeLong(this.getStartItemID());
	}

	public long getStartItemID() {
		return startItemID;
	}

	public void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
	}
}
