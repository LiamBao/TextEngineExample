package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.datanode.client.response.PingResponse;

public class PingRequestHandler implements DNRequestHandler {
	public void handleRequest(DNRequestContext requestContext, InputStream is, OutputStream os)
			throws IOException {
		PingResponse pr = new PingResponse();
		pr.setUpTime(System.currentTimeMillis() - requestContext.getDaemon().getStartTime());
		pr.write(os);
	}
}
