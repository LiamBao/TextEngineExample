package com.cic.textengine.repository.namenode.client.request;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.namenode.NameNodeConst;

public class CleanNameNodeCacheRequest extends NNDaemonRequest{
	
	public CleanNameNodeCacheRequest(){
		this.setType(NameNodeConst.CMD_CLEAN_NN_CACHE);
	}

	@Override
	void readRequestBody(InputStream is) throws IOException {
		
	}

	@Override
	void writeRequestBody(OutputStream os) throws IOException {
		
	}

}
