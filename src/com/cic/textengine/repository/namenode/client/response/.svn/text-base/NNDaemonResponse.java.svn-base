package com.cic.textengine.repository.namenode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.type.Writable;

/**
 * This is the super class for all DataNode daemon response classes.
 * @author denis.yu
 *
 */
public abstract class NNDaemonResponse implements Writable{
	int errorCode = 0;
	String errorMsg = "";
	public int getErrorCode() {
		return errorCode;
	}
	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}
	public String getErrorMsg() {
		return errorMsg;
	}
	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}
	
	void readHeader(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setErrorCode(dis.readInt());
		this.setErrorMsg(dis.readUTF());
	}

	void writeHeader(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(this.getErrorCode());
		if (this.getErrorMsg()==null){
			dos.writeUTF("");
		}else{
			dos.writeUTF(errorMsg);
		}
	}
	
	public void read(InputStream is) throws IOException {
		this.readHeader(is);
		if (this.getErrorCode() == NameNodeConst.ERROR_SUCCESS){
			this.ReadResponseBody(is);
		}
	}

	public void write(OutputStream os) throws IOException {
		this.writeHeader(os);
		if (this.getErrorCode() == NameNodeConst.ERROR_SUCCESS){
			this.WriteResponseBody(os);
		}
	}
	
	
	abstract void WriteResponseBody(OutputStream os) throws IOException;
	
	abstract void ReadResponseBody(InputStream is) throws IOException;
}