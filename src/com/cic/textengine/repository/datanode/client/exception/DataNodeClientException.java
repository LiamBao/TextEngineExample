package com.cic.textengine.repository.datanode.client.exception;

import com.cic.textengine.repository.datanode.client.DataNodeClient;

public class DataNodeClientException extends Exception {
	DataNodeClient m_DNClient = null;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -585342572440807761L;

	public DataNodeClientException(DataNodeClient dn_client, String msg){
		super(msg);
		m_DNClient = dn_client;
	}
	
	public DataNodeClientException(DataNodeClient dn_client, Exception e){
		super(e);
		m_DNClient = dn_client;
	}
	
	public String getMessage(){
		return super.getMessage() + "@[" + m_DNClient.getHost() + "]";
	}
}
