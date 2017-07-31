package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class AssignNNDaemonRequest extends DNDaemonRequest {
	String NNDaemonIP;
	int NNDaemonPort;
	
	public AssignNNDaemonRequest(){
		this.setType(DataNodeConst.CMD_ASSIGN_NN_DAEMON);
	}
	
	@Override
	void readRequestBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setNNDaemonIP(dis.readUTF());
		this.setNNDaemonPort(dis.readInt());
	}

	@Override
	void writeRequestBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeUTF(this.getNNDaemonIP());
		dos.writeInt(this.getNNDaemonPort());
	}

	public String getNNDaemonIP() {
		return NNDaemonIP;
	}

	public void setNNDaemonIP(String daemonIP) {
		NNDaemonIP = daemonIP;
	}

	public int getNNDaemonPort() {
		return NNDaemonPort;
	}

	public void setNNDaemonPort(int daemonPort) {
		NNDaemonPort = daemonPort;
	}

}
