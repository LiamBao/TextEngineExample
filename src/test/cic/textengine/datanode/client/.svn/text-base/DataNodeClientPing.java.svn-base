package test.cic.textengine.datanode.client;

import java.util.ArrayList;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.response.PingResponse;

public class DataNodeClientPing {

	/**
	 * @param args
	 * @throws DataNodeClientCommunicationException 
	 * @throws DataNodeClientException 
	 */
	public static void main(String[] args) throws DataNodeClientException, DataNodeClientCommunicationException {
		ArrayList<String> dnList = new ArrayList<String>();
		dnList.add("192.168.2.101");
//		dnList.add("192.168.2.102");
		dnList.add("192.168.2.103");
		dnList.add("192.168.2.104");
//		dnList.add("192.168.2.115");
		dnList.add("192.168.2.106");
		dnList.add("192.168.2.107");
		dnList.add("192.168.2.108");
		dnList.add("192.168.2.109");
//		dnList.add("192.168.2.110");
		
		dnList.add("192.168.5.102");
		dnList.add("192.168.5.103");
		dnList.add("192.168.5.110");
		dnList.add("192.168.5.111");
		dnList.add("192.168.5.112");
		dnList.add("192.168.5.113");
//		dnList.add("192.168.5.114");
		dnList.add("192.168.5.115");
		int port = 6767;
		for(String dn: dnList){
			DataNodeClient client = new DataNodeClient(dn, port);
			PingResponse res = client.ping();
			System.out.println(dn+":"+res.getErrorCode());
		}

	}

}
