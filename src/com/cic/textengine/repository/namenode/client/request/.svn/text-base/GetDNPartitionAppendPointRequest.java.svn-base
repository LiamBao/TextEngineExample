package com.cic.textengine.repository.namenode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.namenode.NameNodeConst;


public class GetDNPartitionAppendPointRequest extends NNPartitionRequest{
	String DNKey;

	public GetDNPartitionAppendPointRequest(){
		this.setType(NameNodeConst.CMD_GET_DN_PARTITION_APPEND_POINT);
	}
	
	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setDNKey(dis.readUTF());
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeUTF(this.getDNKey());
	}


	public String getDNKey() {
		return DNKey;
	}

	public void setDNKey(String key) {
		DNKey = key;
	}

}

