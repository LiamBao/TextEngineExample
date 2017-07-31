package com.cic.textengine.repository.partitionlock;

public class PartitionAlreadyLockedException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5578688631762242258L;

	PartitionAlreadyLockedException(int year, int month, String siteid, String forumid){
		super("Partition is already locked for writing:[year:" + year
				+ ",month:" + month + ",siteid:" + siteid + ", forumid:"
				+ forumid + "]");
	}
}
