package com.cic.textengine.repository.namenode.client.request;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.type.Writable;

public abstract class NNDaemonRequest implements Writable {

	byte type = 0x00;
	
	
	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	void readHeader(InputStream is) throws IOException{
	}
	
	void writeHeader(OutputStream os) throws IOException{
		byte[] cmd = new byte[1];
		cmd[0] = this.getType();
		os.write(cmd);
	}
	
	public void read(InputStream is) throws IOException {
		readHeader(is);
		readRequestBody(is);
	}

	public void write(OutputStream os) throws IOException {
		writeHeader(os);
		writeRequestBody(os);
	}

	abstract void writeRequestBody(OutputStream os) throws IOException;
	
	abstract void readRequestBody(InputStream is) throws IOException;
}
