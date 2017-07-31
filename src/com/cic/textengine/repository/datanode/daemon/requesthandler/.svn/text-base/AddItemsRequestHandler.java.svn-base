package com.cic.textengine.repository.datanode.daemon.requesthandler;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.TEItemInputStream;
import com.cic.textengine.repository.datanode.client.response.AddItemsResponse;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

public class AddItemsRequestHandler implements DNRequestHandler {
	Logger m_logger = Logger.getLogger(AddItemsRequestHandler.class);
	
	public void handleRequest(DNRequestContext requestContext, InputStream is,
			OutputStream os) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		int year, month, startItemIdx, IDFIdx;
		String siteid, forumid;
		year = dis.readInt();
		month = dis.readInt();
		siteid = dis.readUTF();
		forumid = dis.readUTF();
		startItemIdx = dis.readInt();
		IDFIdx = dis.readInt();
		
		int size = dis.readInt();
		TEItemInputStream teis = null;
		teis = new TEItemInputStream(is);
		ArrayList<TEItem> item_list = new ArrayList<TEItem>();

		for (int i = 0;i<size;i++){
			item_list.add(teis.readItem());
		}

		
		IDFEngine idf = null;
		
		RepositoryEngine rep = null;
		rep = requestContext.getDaemon().getRepositoryEngine();

		try {
			idf = rep.getIDFEngine(year, month, siteid, forumid,IDFIdx);
		} catch (RepositoryEngineException e) {
			m_logger.error(e);

			AddItemsResponse response = new AddItemsResponse();
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg(e.getMessage());
			response.write(os);
			
			return;
		}
		
		//int startItemID = 0;
		try {
			idf.addItems(item_list, startItemIdx);
		} catch (IDFEngineException e) {
			m_logger.error(e);

			AddItemsResponse response = new AddItemsResponse();
			response.setErrorCode(DataNodeConst.ERROR_UNKNOWN);
			response.setErrorMsg(e.getMessage());
			response.write(os);
			
			return;
		}
		
		AddItemsResponse response = new AddItemsResponse();
		response.setErrorCode(DataNodeConst.ERROR_SUCCESS);
		response.setErrorMsg("");
		response.write(os);
	}
}
