package com.cic.textengine.repository.datanode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.datanode.TEItemInputStream;
import com.cic.textengine.repository.datanode.TEItemOutputStream;
import com.cic.textengine.type.TEItem;

public class QueryOneItemResponse extends DNDaemonResponse{

	private TEItem item = null;
	
	public TEItem getItem()
	{
		return this.item;
	}
	
	public void setItem(TEItem item)
	{
		this.item = item;
	}
	
	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		
		DataInputStream dis = new DataInputStream(is);
		TEItemInputStream teis = new TEItemInputStream(dis);
		this.setItem(teis.readItem());
		
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		
		DataOutputStream dos = new DataOutputStream(os);
		TEItemOutputStream teos = new TEItemOutputStream(dos);
		teos.writeTEItem(this.getItem());		
		
	}

}
