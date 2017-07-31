package com.cic.textengine.repository.namenode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.cic.textengine.repository.namenode.NameNodeConst;

public class ApplyPartitionWriteLockRequest extends NNPartitionRequest{
	public static final int OPERATION_APPEND = 1;
	public static final int OPERATION_CLEAN = 2;
	public static final int OPERATION_DELETE = 3;
	
	String DNKey;
	//this is for delete item in partition
	ArrayList<Long> itemIDList = new ArrayList<Long>();
	boolean sorted = false;
	
	int operation = 0;
	
	public ArrayList<Long> getItemIDList(){
		return itemIDList;
	}
	
	public void addItemIDList(ArrayList<Long> list){
		itemIDList.addAll(list);
	}
	
	public ApplyPartitionWriteLockRequest(){
		this.setType(NameNodeConst.CMD_APPLY_PARTITION_WRITE_LOCK);
	}
	
	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setDNKey(dis.readUTF());
		this.setOperation(dis.readInt());
		this.setSorted(dis.readBoolean());
		itemIDList.clear();
		int size = dis.readInt();
		for (int i = 0;i<size;i++){
			itemIDList.add(dis.readLong());
		}
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeUTF(this.getDNKey());
		dos.writeInt(this.getOperation());
		dos.writeBoolean(this.isSorted());
		dos.writeInt(itemIDList.size());
		for (int i = 0;i<itemIDList.size();i++){
			dos.writeLong(itemIDList.get(i));
		}
	}




	public String getDNKey() {
		return DNKey;
	}

	public void setDNKey(String key) {
		DNKey = key;
	}

	public int getOperation() {
		return operation;
	}

	public void setOperation(int operation) {
		this.operation = operation;
	}

	public boolean isSorted() {
		return sorted;
	}

	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}
}
