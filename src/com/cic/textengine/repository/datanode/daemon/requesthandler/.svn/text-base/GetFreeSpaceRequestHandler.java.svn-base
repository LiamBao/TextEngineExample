package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.client.request.GetFreeSpaceRequest;
import com.cic.textengine.repository.datanode.client.response.GetFreeSpaceResponse;

public class GetFreeSpaceRequestHandler implements DNRequestHandler {

	private Logger m_logger = Logger.getLogger(GetFreeSpaceRequestHandler.class);
	
	@Override
	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		GetFreeSpaceRequest request = new GetFreeSpaceRequest();
		request.read(is);
		
		String repoPath = requestContext.getDaemon().m_dataNodeRepositoryFolder;
		File tempFile = new File(repoPath);
		long space = tempFile.getUsableSpace();
		m_logger.debug(String.format(
				"Totally, the free space of DN [%s] is %s ", requestContext
						.getDaemon().getNNDaemonIP(), space));
		
		GetFreeSpaceResponse response = new GetFreeSpaceResponse();
		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setSpace(space);
		response.write(os);
		
	}

}
