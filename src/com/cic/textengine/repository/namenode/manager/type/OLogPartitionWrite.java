package com.cic.textengine.repository.namenode.manager.type;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class OLogPartitionWrite extends OLogItem {
	ArrayList<String> seedDNKeys = new ArrayList<String>();
	long startItemID = 0;
	long itemCount = 0;
	
	byte[] data = null;
	
	public OLogPartitionWrite(){
		this.setType(1);
	}
	
	@Override
	public byte[] getData() throws IOException {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeLong(this.getStartItemID());
		dos.writeLong(this.getItemCount());
		dos.writeInt(seedDNKeys.size());
		for (String dnkey: seedDNKeys){
			dos.writeUTF(dnkey);
		}
		
		byte[] res = baos.toByteArray();
		dos.close();
		return res;
	}
	
	public void readFields(InputStream is) throws IOException{
		DataInputStream dis = new DataInputStream(is);
		this.setStartItemID(dis.readLong());
		this.setItemCount(dis.readLong());
		int size = dis.readInt();
		seedDNKeys.clear();
		for (int i = 0;i<size;i++){
			this.addSeedDNKey(dis.readUTF());
		}
	}
	
	public void addSeedDNKey(String dnkey){
		seedDNKeys.add(dnkey);
	}
	
	public void addSeedDNKeys(ArrayList<String> dnkeys){
		seedDNKeys.addAll(dnkeys);
	}
	
	public void cleanSeedDNKeys(){
		seedDNKeys.clear();
	}
	
	public ArrayList<String> listSeedDNKeys(){
		return seedDNKeys;
	}
	
	
	public long getStartItemID() {
		return startItemID;
	}

	public void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
	}



	public long getItemCount() {
		return itemCount;
	}



	public void setItemCount(long itemCount) {
		this.itemCount = itemCount;
	}

}
