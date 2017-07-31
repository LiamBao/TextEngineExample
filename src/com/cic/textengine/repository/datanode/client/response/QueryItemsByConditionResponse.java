package com.cic.textengine.repository.datanode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import com.cic.textengine.type.TEItem;

public class QueryItemsByConditionResponse extends DNDaemonResponse{

	ArrayList<TEItem> itemList = new ArrayList<TEItem>();
	int itemCount = 0;
	
	public int getItemCount() {
		return itemCount;
	}

	public void setItemCount(int itemCount) {
		this.itemCount = itemCount;
	}

	public ArrayList<TEItem> getItemList() {
		return itemList;
	}

	public void setItemList(ArrayList<TEItem> itemList) {
		this.itemList = itemList;
	}

	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		itemCount = dis.readInt();
		int count = 0;
		while(count < itemCount){
			TEItem item = new TEItem();
			item.readFields(dis);
			itemList.add(item);
			count ++;
		}
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(itemCount);
		for(TEItem item: itemList){
			item.write(dos);
		}
	}

}
