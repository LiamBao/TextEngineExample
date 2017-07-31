package com.cic.textengine.repository.datanode.daemon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.daemon.requesthandler.DNRequestContext;
import com.cic.textengine.repository.datanode.daemon.requesthandler.DNRequestHandler;


public class DNRequestHandlerThread extends Thread{
	Logger m_logger = Logger.getLogger(DNRequestHandlerThread.class);
	
	DNRequestHandler m_requestHandler = null;
	
	DNRequestContext m_requestContext = null;
	InputStream m_inputStream = null;
	OutputStream m_outputStream = null;
	Socket m_socket = null;
	
	DNRequestHandlerThread(DNRequestHandler rh, DNRequestContext requestContext, 
			Socket socket, InputStream is, OutputStream os){
		m_requestHandler = rh;
		m_requestContext = requestContext;
		m_inputStream = is;
		m_outputStream = os;
		m_socket = socket;
	}
	
	public void run(){
		m_requestContext.getDaemon().increaseActiveThreads();
		try {
			m_requestHandler.handleRequest(m_requestContext, m_inputStream, m_outputStream);
			m_inputStream.close();
			m_outputStream.close();
			m_socket.close();
		} catch (IOException e) {
			m_logger.error("Exception when processing request:", e);
		}finally{
			m_requestContext.getDaemon().decreaseActiveThreads();
		}
	}
}
