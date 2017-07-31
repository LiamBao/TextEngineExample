package com.cic.textengine.repository.datanode.client.request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.cic.textengine.repository.datanode.DataNodeConst;
import com.cic.textengine.repository.datanode.TEItemInputStream;
import com.cic.textengine.type.TEItem;

public class AddItemsRequest extends DNPartitionRequest {
	int startItemIdx, IDFIdx;
	
	ArrayList<TEItem> m_itemList = new ArrayList<TEItem>();
	
	public AddItemsRequest(){
		this.setType(DataNodeConst.CMD_ADD_ITEMS);
	}
	
	@Override
	void readPartitionRequestBody(DataInputStream dis) throws IOException {
		this.setStartItemIdx(dis.readInt());
		this.setIDFIdx(dis.readInt());

		this.clearTEItemList();
		int size = dis.readInt();
		TEItemInputStream teis = null;
		teis = new TEItemInputStream(dis);
		ArrayList<TEItem> item_list = new ArrayList<TEItem>();

		for (int i = 0;i<size;i++){
			item_list.add(teis.readItem());
		}
	}

	@Override
	void writePartitionRequestBody(DataOutputStream dos) throws IOException {
		dos.writeInt(this.getStartItemIdx());
		dos.writeInt(this.getIDFIdx());
		
		dos.writeInt(listItems().size());
		for (int i = 0;i<listItems().size();i++){
			listItems().get(i).write(dos);
		}
	}

	public ArrayList<TEItem> listItems(){
		return m_itemList;
	}
	
	public void addTEItemList(ArrayList<TEItem> list){
		m_itemList.addAll(list);
	}
	
	public void addTEItem(TEItem item){
		m_itemList.add(item);
	}
	
	public void clearTEItemList(){
		m_itemList.clear();
	}


	public int getStartItemIdx() {
		return startItemIdx;
	}

	public void setStartItemIdx(int startItemIdx) {
		this.startItemIdx = startItemIdx;
	}

	public int getIDFIdx() {
		return IDFIdx;
	}

	public void setIDFIdx(int idx) {
		IDFIdx = idx;
	}


}
