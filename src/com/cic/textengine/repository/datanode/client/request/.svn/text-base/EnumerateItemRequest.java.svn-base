package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.datanode.DataNodeConst;


public class EnumerateItemRequest extends DNPartitionRequest {
	boolean includeDeletedItems = false;
	long startItemID = 1;
	long itemCount = 0;//0 means enumerate all items.

	public long getStartItemID() {
		return startItemID;
	}

	public void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
	}

	public EnumerateItemRequest(){
		this.setType(DataNodeConst.CMD_ENUMERATE_ITEM);
	}
	
	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setIncludeDeletedItems(dis.readBoolean());
		this.setStartItemID(dis.readLong());
		this.setItemCount(dis.readLong());
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeBoolean(this.isIncludeDeletedItems());
		dos.writeLong(this.getStartItemID());
		dos.writeLong(this.getItemCount());
	}
	

	public boolean isIncludeDeletedItems() {
		return includeDeletedItems;
	}

	public void setIncludeDeletedItems(boolean includeDeletedItems) {
		this.includeDeletedItems = includeDeletedItems;
	}

	public long getItemCount() {
		return itemCount;
	}

	public void setItemCount(long itemCount) {
		this.itemCount = itemCount;
	}

}
