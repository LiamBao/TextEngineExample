package com.cic.textengine.repository.namenode.manager.type;

import java.io.IOException;
import java.io.InputStream;

public class OLogPartitionClean extends OLogItem {

	public OLogPartitionClean(){
		this.setType(2);
	}
	
	public void readFields(InputStream is) throws IOException{
	}

	@Override
	public byte[] getData() throws IOException {
		return new byte[0];
	}
}
