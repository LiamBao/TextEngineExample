package com.cic.textengine.repository;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class StopItemImporter {
	
	private static Logger logger = Logger.getLogger(StopItemImporter.class);
	
	public static void main(String[] args)
	{
		if(args.length < 1)
		{
			System.out.println("StopItemImporter [ItemImporter Address]");
			return;
		}
		
		String address = args[0];
		int port = ItemImporter.DAEMON_PORT;
		
		try {
			Socket socket = new Socket(address, port);
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.writeInt(ItemImporter.STOP_SIGNAL);
			dos.close();
			socket.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
