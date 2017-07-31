package com.cic.textengine.client.exception;

public class TEItemEnumeratorException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1274125312250751736L;
	
	public TEItemEnumeratorException(String msg)
	{
		super(msg);
	}
	
	public TEItemEnumeratorException(Exception e)
	{
		super(e);
	}

}
