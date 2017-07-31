package com.cic.textengine.repository.datanode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import com.cic.textengine.type.TEItem;

public class DeleteItemsByConditionResponse extends DNDaemonResponse{

	long deleteCount = 0;
	ArrayList<TEItem> itemList = new ArrayList<TEItem>();
	
	public ArrayList<TEItem> getItemList() {
		return itemList;
	}

	public void setItemList(ArrayList<TEItem> itemList) {
		this.itemList = itemList;
	}

	public long getDeleteCount() {
		return deleteCount;
	}

	public void setDeleteCount(long deleteCount) {
		this.deleteCount = deleteCount;
	}

	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setDeleteCount(dis.readLong());
		int count = 0;
		while(count < deleteCount){
			TEItem item = new TEItem();
			item.readFields(dis);
			itemList.add(item);
		}
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeLong(this.getDeleteCount());
		for(TEItem item: itemList){
			item.write(dos);
		}
	}

}
