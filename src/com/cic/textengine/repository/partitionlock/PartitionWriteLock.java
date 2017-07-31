package com.cic.textengine.repository.partitionlock;

public class PartitionWriteLock {
	String IP;
	String dataNodeKey;
	//the operation type: 1:append, 2: clean, 3:delete
	int operation = 0;
	
	int year, month;
	String siteID, forumID;
	
	long createTimestamp = System.currentTimeMillis();
	
	public long getCreateTimestamp() {
		return createTimestamp;
	}

	PartitionWriteLock(int year, int month, String siteid, String forumid, String IP, String NameNodeKey, int operation){
		this.setYear(year);
		this.setMonth(month);
		this.setSiteID(siteid);
		this.setForumID(forumid);
		this.setIP(IP);
		this.setDataNodeKey(NameNodeKey);
		this.setOperation(operation);
	}
	
	public String getIP() {
		return IP;
	}
	public void setIP(String ip) {
		IP = ip;
	}
	public String getDataNodeKey() {
		return dataNodeKey;
	}
	public void setDataNodeKey(String dataNodeKey) {
		this.dataNodeKey = dataNodeKey;
	}

	public int getOperation() {
		return operation;
	}

	public void setOperation(int operation) {
		this.operation = operation;
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
	
	public String toString(){
		String res = "PWL[Y:" + this.getYear() + ",M:" + this.getMonth()
				+ ",S:" + this.getSiteID() + ",F:" + this.getForumID() + ",O:"
				+ this.getOperation() + "]";
		return res;
	}
}
