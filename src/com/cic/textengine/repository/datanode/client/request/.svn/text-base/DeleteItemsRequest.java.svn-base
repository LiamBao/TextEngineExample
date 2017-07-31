package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.cic.textengine.repository.datanode.DataNodeConst;

public class DeleteItemsRequest extends DNPartitionRequest{
	boolean sorted;

	ArrayList<Long> itemid_list = new ArrayList<Long>();

	public DeleteItemsRequest(){
		this.setType(DataNodeConst.CMD_REMOVE_ITEMS);
	}
	
	public void clearItemIDList(){
		itemid_list.clear();
	}
	
	public ArrayList<Long> listItemIDs(){
		return itemid_list;
	}
	
	public void addItemIDList(ArrayList<Long> list){
		itemid_list.addAll(list);
	}
	
	public void addItemID(long itemid){
		itemid_list.add(itemid);
	}

	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setSorted(dis.readBoolean());
		
		this.clearItemIDList();
		int size = dis.readInt();
		for (int i = 0;i<size;i++){
			this.addItemID(dis.readLong());
		}
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeBoolean(this.isSorted());
		
		dos.writeInt(itemid_list.size());
		for (int i = 0;i<itemid_list.size();i++){
			dos.writeLong(itemid_list.get(i));
		}
	}

	public boolean isSorted() {
		return sorted;
	}

	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}
}
