package com.cic.textengine.repository.namenode.manager.type;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class OLogPartitionDelete extends OLogItem {
	ArrayList<Long> itemIDList = new ArrayList<Long>();
	boolean sorted = false;
	
	public boolean isSorted() {
		return sorted;
	}

	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}

	public void addItemIDList(ArrayList<Long> idlist){
		itemIDList.addAll(idlist);
	}
	
	public void addItemID(long id){
		itemIDList.add(id);
	}
	
	public OLogPartitionDelete(){
		this.setType(3);
	}
	
	public ArrayList<Long> listItemIDs(){
		return itemIDList;
	}


	public void readFields(InputStream is) throws IOException{
		DataInputStream dis = new DataInputStream(is);
		this.setSorted(dis.readBoolean());
		int size = dis.readInt();
		itemIDList.clear();
		for (int i = 0;i<size;i++){
			this.addItemID(dis.readLong());
		}
	}

	@Override
	public byte[] getData() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		dos.writeBoolean(this.isSorted());
		dos.writeInt(itemIDList.size());
		for (int i = 0;i<itemIDList.size();i++){
			dos.writeLong(itemIDList.get(i));
		}
		byte[] res = baos.toByteArray();
		dos.close();
		return res;
	}

}
