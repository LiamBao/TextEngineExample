package com.cic.textengine.repository.namenode.dnregistry;

public class DNRegistry {
	static long DEFAULT_TTL = 600000;
	String DNKey;
	String host;
	int port;
	long TTL = DEFAULT_TTL;

	int writingCount = 0;
	int readingCount = 0;
	int partitionCount = 0;
	long freeSpace = 0;
	
	public long getFreeSpace() {
		return freeSpace;
	}
	public void setFreeSpace(long freeSpace) {
		this.freeSpace = freeSpace;
	}
	public String getDNKey() {
		return DNKey;
	}
	public void setDNKey(String key) {
		DNKey = key;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

	public void increaseReadingCount(){
		this.setReadingCount(this.readingCount + 1);
	}
	
	public void decreaseReadingCount(){
		this.setReadingCount(this.readingCount - 1);
	}
	
	public int getReadingCount() {
		return readingCount;
	}
	public void setReadingCount(int readingCount) {
		this.readingCount = readingCount;
	}
	public long getTTL() {
		return TTL;
	}
	public void setTTL(long ttl) {
		TTL = ttl;
	}
	public void resetTTL() {
		this.setTTL(DEFAULT_TTL);
		
	}
	
	public void increaseWritingCount(){
		this.setWritingCount(this.writingCount + 1);
	}
	
	public void decreaseWritingCount(){
		this.setWritingCount(this.writingCount - 1);
	}
	
	public int getWritingCount() {
		return writingCount;
	}
	public void setWritingCount(int writingCount) {
		this.writingCount = writingCount;
	}
	public int getPartitionCount() {
		return partitionCount;
	}
	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}
	
	public void increasePartitionCount(){
		this.partitionCount++;
	}
}
