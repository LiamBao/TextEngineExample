package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class QueryOneItemRequest extends DNPartitionRequest{
	
	private long itemid = 0;
	
	public QueryOneItemRequest()
	{
		this.setType(DataNodeConst.CMD_QUERY_ONE_ITEM);
	}
	public long getItemID()
	{
		return itemid;
	}
	
	public void setItemID(long id)
	{
		this.itemid = id;
	}

	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {

		setItemID(dis.readLong());
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {

		dos.writeLong(this.getItemID());
	}

}
