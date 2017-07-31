package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class AssignKeyRequest extends DNDaemonRequest {
	String key = null;
	
	public AssignKeyRequest(){
		this.setType(DataNodeConst.CMD_ASSIGN_DN_KEY);
	}
	
	@Override
	void readRequestBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setKey(dis.readUTF());

	}

	@Override
	void writeRequestBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeUTF(this.getKey());
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}


}
