package com.cic.textengine.repository.namenode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class NNPartitionRequest extends NNDaemonRequest {
	int year, month;
	String siteID, forumID;

	abstract void readPartitionRequestBody(DataInputStream dis)
			throws IOException;

	abstract void writePartitionRequestBody(DataOutputStream dos)
			throws IOException;

	@Override
	void readRequestBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setYear(dis.readInt());
		this.setMonth(dis.readInt());
		this.setSiteID(dis.readUTF());
		this.setForumID(dis.readUTF());
		readPartitionRequestBody(dis);
	}

	@Override
	void writeRequestBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(this.getYear());
		dos.writeInt(this.getMonth());
		dos.writeUTF(this.getSiteID());
		dos.writeUTF(this.getForumID());
		writePartitionRequestBody(dos);
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public String getSiteID() {
		return siteID;
	}

	public void setSiteID(String siteID) {
		this.siteID = siteID;
	}

	public String getForumID() {
		return forumID;
	}

	public void setForumID(String forumID) {
		this.forumID = forumID;
	}

}
