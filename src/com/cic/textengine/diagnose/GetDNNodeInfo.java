package com.cic.textengine.diagnose;

import java.util.ArrayList;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.response.PingResponse;

public class GetDNNodeInfo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ArrayList<String> dnList = new ArrayList<String>();
		dnList.add("192.168.2.101");
		dnList.add("192.168.2.102");
		dnList.add("192.168.2.103");
		dnList.add("192.168.2.104");
		dnList.add("192.168.2.106");
		dnList.add("192.168.2.107");
		dnList.add("192.168.2.108");
		dnList.add("192.168.2.109");
		dnList.add("192.168.2.115");
		dnList.add("192.168.2.116");
		
		dnList.add("192.168.5.102");
		dnList.add("192.168.5.103");
		dnList.add("192.168.5.110");
		dnList.add("192.168.5.111");
		dnList.add("192.168.5.112");
		dnList.add("192.168.5.113");
		dnList.add("192.168.5.115");
		dnList.add("192.168.5.128");
		int port = 6767;
		for(String dn: dnList){
			DataNodeClient client = new DataNodeClient(dn, port);
			PingResponse res;
			try {
				res = client.ping();
				int errorCode = res.getErrorCode();
				if(errorCode == 0)
				{
					long space = client.getFreeSpace();
					System.out.println(dn+":"+space/(1024*1024*1024)+"G");
				} else {
					System.out.println(String.format("DN %s is down", dn));
				}
			} catch (DataNodeClientException e) {
				System.out.println(String.format("DN %s is down", dn));
			} catch (DataNodeClientCommunicationException e) {
				System.out.println(String.format("DN %s is down", dn));
			}
		}

	}

}
