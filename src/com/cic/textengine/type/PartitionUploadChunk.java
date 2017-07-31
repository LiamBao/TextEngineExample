package com.cic.textengine.type;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PartitionUploadChunk {
	private String partitionKey = null;
	private long startItemID = 0;
	private int itemCount = 0;
	public PartitionUploadChunk(String partitionKey, long startItemID, int itemCount){
		this.partitionKey = partitionKey;
		this.startItemID = startItemID;
		this.itemCount = itemCount;
	}
	public String getPartitionKey() {
		return partitionKey;
	}
	public long getStartItemID() {
		return startItemID;
	}
	public int getItemCount() {
		return itemCount;
	}
	public void write(DataOutputStream out) throws IOException {
		out.writeUTF(partitionKey);
		out.writeLong(startItemID);
		out.writeInt(itemCount);
	}
	
	public static PartitionUploadChunk read(DataInputStream in) throws IOException {
		String partitionKey = in.readUTF();
		long startItemID = in.readLong();
		int itemCount = in.readInt();
		PartitionUploadChunk chunk = new PartitionUploadChunk(partitionKey, startItemID, itemCount);
		return chunk;
	}
}
