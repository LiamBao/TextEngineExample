package com.cic.textengine.repository.datanode.repository.exception;

public class RepositoryEngineException extends Exception {

	int year, month;
	String siteID, forumID;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5807887121260393240L;

	public RepositoryEngineException(String msg){
		super(msg);
	}
	
	public RepositoryEngineException(Exception e){
		super(e);
	}

	public void setParititonKey(int year, int month, String siteid, String forumid){
		this.year = year;
		this.month = month;
		this.siteID = siteid;
		this.forumID = forumid;
	}
	
	public String getMessage(){
		if (this.siteID == null){
			return super.getMessage();
		}else{
			return super.getMessage() + " for PK[y:" + this.year + ",m:" + this.month +
			",s:" + this.siteID + ",f:" + this.forumID + "]";
		}
	}
}
