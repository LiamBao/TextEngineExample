package com.cic.textengine.type.exception;

public class XMLParsingException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4072536321417690782L;
	
	private String itempath = null;

	public XMLParsingException(String msg){
		super(msg);
	}
	
	public XMLParsingException(Exception ex){
		super(ex);
	}
	
	public void setItemPath(String path)
	{
		this.itempath = path;
	}
	
	public String getItemPath()
	{
		return this.itempath;
	}
}
