package com.cic.textengine.diagnose;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.response.PingResponse;

public class PingDN {

	/**
	 * @param args
	 * @throws DataNodeClientCommunicationException 
	 * @throws DataNodeClientException 
	 */
	public static void main(String[] args) throws DataNodeClientException, DataNodeClientCommunicationException {
		if(args.length < 2){
			System.out.println("2 parameters needed: address port");
			return;
		}
		String addr = args[0].trim();
		int port = Integer.parseInt(args[1].trim());
		DataNodeClient client = new DataNodeClient(addr, port);
		PingResponse res = client.ping();
		System.out.println(res.getErrorCode());
	}

}
