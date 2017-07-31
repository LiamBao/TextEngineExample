package com.cic.textengine.tefilter.exception;

public class TEItemFilterException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4122559258994531336L;
	
	public TEItemFilterException(Exception e)
	{
		super(e);
	}
	
	public TEItemFilterException(String msg)
	{
		super(msg);
	}

}
