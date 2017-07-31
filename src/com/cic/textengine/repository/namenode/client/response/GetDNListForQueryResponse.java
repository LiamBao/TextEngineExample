package com.cic.textengine.repository.namenode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class GetDNListForQueryResponse extends NNDaemonResponse{
	
	private ArrayList<String> dnList ;

	public ArrayList<String> getDnList() {
		return dnList;
	}

	public void setDnList(ArrayList<String> dnList) {
		this.dnList = dnList;
	}

	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		int count = dis.readInt();
		this.dnList = new ArrayList<String>();
		for(int i=0; i<count; i++){
			dnList.add(dis.readUTF());
		}
		
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		if(this.dnList != null)
		{
			dos.writeInt(this.dnList.size());
			for(String dn: dnList){
				dos.writeUTF(dn);
			}
		}
		
	}

}
