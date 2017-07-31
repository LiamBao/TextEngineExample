package com.cic.textengine.repository.importer.exception;

public class UploadTEItemNotMatchLocalTEItemException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6228298175855210489L;

	public UploadTEItemNotMatchLocalTEItemException(String field, String remoteValue, String localValue){
		super("Field [" + field + "] doesn't match. Remote Value["
				+ remoteValue + "],Local Value[" + localValue + "]");
	}
}
