package com.cic.textengine.repository.namenode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.namenode.NameNodeConst;


public class DeactivateDataNodeRequest extends NNDaemonRequest{
	String DNKey;

	public DeactivateDataNodeRequest(){
		this.setType(NameNodeConst.CMD_DEACTIVATE_DATA_NODE);
	}
	
	@Override
	void readRequestBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setDNKey(dis.readUTF());
	}

	@Override
	void writeRequestBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeUTF(this.getDNKey());
	}


	public String getDNKey() {
		return DNKey;
	}

	public void setDNKey(String key) {
		DNKey = key;
	}

}

