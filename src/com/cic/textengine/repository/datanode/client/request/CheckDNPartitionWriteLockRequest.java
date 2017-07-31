package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class CheckDNPartitionWriteLockRequest extends DNPartitionRequest{
	public CheckDNPartitionWriteLockRequest(){
		this.setType(DataNodeConst.CMD_CHECK_DN_PARTITION_WRITE_LOCK);
	}
	

	@Override
	void readPartitionRequestBody(DataInputStream is) throws IOException {
	}

	@Override
	void writePartitionRequestBody(DataOutputStream os) throws IOException {
	}
	
}
