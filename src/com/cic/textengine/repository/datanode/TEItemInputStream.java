package com.cic.textengine.repository.datanode;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.cic.textengine.type.TEItem;

public class TEItemInputStream extends InputStream {
	DataInputStream m_dataInputStream = null;
	
	public TEItemInputStream(InputStream is){
		m_dataInputStream = new DataInputStream(is);
	}

	
	@Override
	public int read() throws IOException {
		throw new IOException("This method is not allowed here. Use readTEItem istead.");
	}

	
	public TEItem readItem() throws IOException{
		TEItem item = new TEItem();
		item.readFields(m_dataInputStream);
		return item;
	}
	
	
	public void close() throws IOException{
		if (m_dataInputStream != null){
			m_dataInputStream.close();
		}
	}
}
