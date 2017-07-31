package com.cic.textengine.repository.namenode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.namenode.NameNodeConst;

public class GetDNClientForQueryRequest extends NNPartitionRequest{


	public GetDNClientForQueryRequest(){
		this.setType(NameNodeConst.CMD_GET_DN_CLIENT_FOR_QUERY);
	}



	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
	}
	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
	}
}
