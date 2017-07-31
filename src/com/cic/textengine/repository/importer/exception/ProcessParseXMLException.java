package com.cic.textengine.repository.importer.exception;

public class ProcessParseXMLException extends ImporterProcessException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2844986174449449322L;

	public ProcessParseXMLException(Exception e){
		super(e);
	}
	
	public ProcessParseXMLException(String msg){
		super(msg);
	}
}
