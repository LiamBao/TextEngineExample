package com.cic.textengine.type.exception;

/**
 * Exception for TEItem serialization or deserialization process.
 * 
 * @author CICDATA\denis.yu
 *
 */
public class TEItemSerializationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3817672338500033630L;
	
	
	public TEItemSerializationException(String msg){
		super(msg);
	}
	
	public TEItemSerializationException(Exception ex){
		super(ex);
	}

}
