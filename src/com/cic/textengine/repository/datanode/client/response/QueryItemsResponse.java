package com.cic.textengine.repository.datanode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import com.cic.textengine.repository.datanode.TEItemInputStream;
import com.cic.textengine.repository.datanode.TEItemOutputStream;
import com.cic.textengine.type.TEItem;

public class QueryItemsResponse extends DNDaemonResponse {
	ArrayList<TEItem> m_itemList = new ArrayList<TEItem>();
	
	public void addTEItem(TEItem item){
		m_itemList.add(item);
	}
	
	public void addTEItemList(ArrayList<TEItem> list){
		m_itemList.addAll(list);
	}
	
	public void cleanList(){
		m_itemList.clear();
	}
	
	
	public ArrayList<TEItem> getTEItemList(){
		return m_itemList;
	}
	
	@Override
	void ReadResponseBody(InputStream is) throws IOException {
		this.cleanList();
		DataInputStream dis = new DataInputStream(is);
		TEItemInputStream teis = new TEItemInputStream(dis);
		int size = dis.readInt();
		for (int i = 0;i<size;i++){
			this.addTEItem(teis.readItem());
		}
	}

	@Override
	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		TEItemOutputStream teos = new TEItemOutputStream(dos);
		dos.writeInt(m_itemList.size());
		for (int i = 0;i<m_itemList.size();i++){
			teos.writeTEItem(m_itemList.get(i));
		}
	}

}
