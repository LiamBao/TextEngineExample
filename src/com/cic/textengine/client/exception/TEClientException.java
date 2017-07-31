package com.cic.textengine.client.exception;

public class TEClientException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7434042137742357003L;
	
	public TEClientException(String msg)
	{
		super(msg);
	}
	
	public TEClientException(Exception e)
	{
		super(e);
	}

}
