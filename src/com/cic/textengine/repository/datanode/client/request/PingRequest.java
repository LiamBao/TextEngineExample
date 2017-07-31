package com.cic.textengine.repository.datanode.client.request;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class PingRequest extends DNDaemonRequest {

	public PingRequest(){
		this.setType(DataNodeConst.CMD_PING);
	}
	
	@Override
	void readRequestBody(InputStream is) throws IOException {
		//do nothing
	}

	@Override
	void writeRequestBody(OutputStream os) throws IOException {
		//do nothing
	}

}
