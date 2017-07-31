package com.cic.textengine.repository.importer.exception;

public class ProcessUploadItemException extends ImporterProcessException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2844986174449449322L;

	public ProcessUploadItemException(Exception e){
		super(e);
	}
	
	public ProcessUploadItemException(String msg){
		super(msg);
	}
}
