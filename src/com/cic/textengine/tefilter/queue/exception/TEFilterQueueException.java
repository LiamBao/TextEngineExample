package com.cic.textengine.tefilter.queue.exception;

public class TEFilterQueueException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6920897081848910851L;

	public TEFilterQueueException (Exception e) {
		super(e);
	}
	
	public TEFilterQueueException (String msg) {
		super(msg);
	}
	
}
