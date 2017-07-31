package com.cic.textengine.repository.importer.exception;

public class ProcessIsolationException extends ImporterProcessException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2805329678280412547L;
	
	public ProcessIsolationException(Exception e) {
		super(e);
	}
	
	public ProcessIsolationException(String msg) {
		super(msg);
	}

}
