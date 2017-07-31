package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface DNRequestHandler {
	
	/**
	 * Handle request
	 *
	 * All exceptions except IOException should be handled internal this method by 
	 * printing error log and send back error response.
	 * 
	 * @param requestContext
	 * @param is
	 * @param os
	 * @throws IOException
	 */
	public void handleRequest(DNRequestContext requestContext, InputStream is, OutputStream os)
		throws IOException;
}
