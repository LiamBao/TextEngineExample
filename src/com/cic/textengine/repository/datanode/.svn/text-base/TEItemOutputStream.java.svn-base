package com.cic.textengine.repository.datanode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.cic.textengine.type.TEItem;

public class TEItemOutputStream extends OutputStream {

	DataOutputStream m_dataOutputStream = null;
	
	public TEItemOutputStream(OutputStream os){
		m_dataOutputStream = new DataOutputStream(os);
	}
	
	@Override
	public void write(int arg0) throws IOException {
		throw new IOException("This function is not supported here, use writeTEItem instead.");
	}

	public void writeTEItem(TEItem item)
	throws IOException{
		item.write(m_dataOutputStream);
	}
}
