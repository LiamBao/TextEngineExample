package com.cic.textengine.repository.namenode.manager.exception;

public class IllegalNameNodeException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8765663088857734392L;

	public IllegalNameNodeException(String dndaemon_ip, String key) {
		super("Illegal name node found.[key:" + key + ", ip:" + dndaemon_ip + "]");
	}

}
