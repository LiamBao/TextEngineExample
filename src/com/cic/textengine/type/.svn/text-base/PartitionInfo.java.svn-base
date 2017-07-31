package com.cic.textengine.type;

public class PartitionInfo {
	/**
	 * The number of segments within the partition
	 */
	private int numOfSegments;

	/**
	 * The size of the active segment
	 */
	private int activeSize;

	public int getActiveSize() {
		return activeSize;
	}

	public int getNumOfSegments() {
		return numOfSegments;
	}

	public void increaseNumOfSegments() {
		this.numOfSegments++;
	}

	public void increaseSize(int size) {
		this.activeSize += size;
	}

	public void setActiveSize(int activeSize) {
		this.activeSize = activeSize;
	}

	public void setNumOfSegments(int numOfSegments) {
		this.numOfSegments = numOfSegments;
	}
}