package com.cic.textengine.repository.importer.exception;

public class ProcessIndexItemException extends ImporterProcessException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6300099751982017182L;

	public ProcessIndexItemException(Exception e){
		super(e);
	}
	
	public ProcessIndexItemException(String msg){
		super(msg);
	}
}