package com.cic.textengine.repository.namenode.daemon.requesthandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.namenode.NameNodeConst;
import com.cic.textengine.repository.namenode.client.request.DeactivateDataNodeRequest;
import com.cic.textengine.repository.namenode.client.response.DeactivateDataNodeResponse;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;

public class DeactivateDataNodeRequestHandler implements NNRequestHandler{

	public void handleRequest(NNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		DeactivateDataNodeRequest request = new DeactivateDataNodeRequest();
		request.read(is);

		DNRegistryTable.getInstance().unregisterDN(request.getDNKey());

		DeactivateDataNodeResponse response = 
			new DeactivateDataNodeResponse();
		response.setErrorCode(NameNodeConst.ERROR_SUCCESS);
		response.write(os);
	}

}
