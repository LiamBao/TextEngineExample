package com.cic.textengine.repository.namenode.daemon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.daemon.requesthandler.NNRequestContext;
import com.cic.textengine.repository.namenode.daemon.requesthandler.NNRequestHandler;

public class NNRequestHandlerThread extends Thread{
	Logger m_logger = Logger.getLogger(NNRequestHandlerThread.class);
	
	NNRequestHandler m_requestHandler = null;
	
	NNRequestContext m_requestContext = null;
	InputStream m_inputStream = null;
	OutputStream m_outputStream = null;
	Socket m_socket = null;
	
	NNRequestHandlerThread(NNRequestHandler rh, NNRequestContext requestContext, 
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
			m_logger.error(e);
		}finally{
			m_requestContext.getDaemon().decreaseActiveThreads();
		}
	}
}
