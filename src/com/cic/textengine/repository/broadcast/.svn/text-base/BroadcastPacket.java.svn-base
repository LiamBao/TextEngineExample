package com.cic.textengine.repository.broadcast;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.type.Writable;

public abstract class BroadcastPacket implements Writable{
	public static final byte TYPE_BCPKG_DN_KEY = 0x01;
	
	static final int TR_BROADCAST_PACKAGE_TAG = 0x0A952B81;

	byte packageType;
	
	
	public byte getPackageType() {
		return packageType;
	}

	void setPackageType(byte packageType) {
		this.packageType = packageType;
	}

	public final void read(InputStream is) throws IOException {
		readBody(is);
	}

	public abstract void readBody(InputStream is) throws IOException;
	
	public final void write(OutputStream os) throws IOException {
		writeHeader(os);
		writeBody(os);
	}

	void writeHeader(OutputStream os) throws IOException{
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(TR_BROADCAST_PACKAGE_TAG);
		dos.writeByte(this.getPackageType());
	}
	
	public abstract void writeBody(OutputStream os) throws IOException;
}
