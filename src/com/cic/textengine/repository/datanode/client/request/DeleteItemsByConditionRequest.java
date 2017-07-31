package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class DeleteItemsByConditionRequest extends DNPartitionRequest{

	String condition = null;
	
	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}
	
	public DeleteItemsByConditionRequest() {
		this.setType(DataNodeConst.CMD_DELETE_ITEMS_BYCONDITION);
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
