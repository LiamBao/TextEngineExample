package com.cic.textengine.repository.datanode.client.response;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class DeleteItemsResponse extends DNDaemonResponse{

	@Override
	void ReadResponseBody(InputStream is) throws IOException {
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
	}
	
}
