package com.cic.textengine.repository.datanode.client.exception;

/**
 * Throw this exception if the client can not talk to the 
 * DNDaemon because of the communication issue.
 * 
 * @author denis.yu
 *
 */
public class DataNodeClientCommunicationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1299090503512011625L;

	public DataNodeClientCommunicationException(Exception e){
		super(e);
	}
}
