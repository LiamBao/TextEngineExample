package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class QueryItemsByConditionRequest extends DNPartitionRequest{

	String condition = null;
	
	public QueryItemsByConditionRequest() {
		this.setType(DataNodeConst.CMD_QUERY_ITEMS_BYCONDITION);
	}
	
	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setCondition(dis.readUTF());		
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeUTF(this.getCondition());
	}

}
