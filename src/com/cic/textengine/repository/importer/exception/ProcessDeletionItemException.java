package com.cic.textengine.repository.importer.exception;

public class ProcessDeletionItemException extends ImporterProcessException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6300099751982017182L;

	public ProcessDeletionItemException(Exception e){
		super(e);
	}
	
	public ProcessDeletionItemException(String msg){
		super(msg);
	}
}
