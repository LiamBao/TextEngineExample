package com.cic.textengine.repository.namenode.client.response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import com.cic.textengine.repository.namenode.manager.type.OLogItem;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionClean;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionDelete;
import com.cic.textengine.repository.namenode.manager.type.OLogPartitionWrite;

public class GetNextDNPartitionOperationResponse extends NNDaemonResponse{
	//0: means the operation is failed, then release the lock
	//1: append, 2:clean, 3: delete
	int operation = 0;
	int partitionID = 0;
	
	//the following properties are used by append operation
	long startItemID = 0;
	long itemCount = 0;
	String seedDNKey = "";
	String seedDNHost = "";
	int seedDNPort = 0;
	
	//the following properties are used by delete aperation
	ArrayList<Long> itemIDList = new ArrayList<Long>();
	boolean sorted = false;
	
	//the following properties are used by delete operation.
	int version = 0;
	
	public OLogItem getOLogItem(){
		OLogItem result = null;
		
		switch(this.getOperation()){
		case 0://no operation requried
			return null;
		case 1:
			OLogPartitionWrite log_write = new OLogPartitionWrite();
			log_write.setItemCount(this.getItemCount());
			log_write.addSeedDNKey(this.getSeedDNKey());
			log_write.setStartItemID(this.getStartItemID());
			log_write.setVersion(this.getVersion());
			log_write.setPartitionID(this.getPartitionID());
			result = log_write;
			break;
		case 2:
			OLogPartitionClean log_clean = new OLogPartitionClean();
			log_clean.setPartitionID(this.getPartitionID());
			log_clean.setVersion(this.getVersion());
			result = log_clean;
			break;
		case 3:
			OLogPartitionDelete log_delete = new OLogPartitionDelete();
			log_delete.setPartitionID(this.getPartitionID());
			log_delete.setVersion(this.getVersion());
			log_delete.setSorted(this.isSorted());
			log_delete.addItemIDList(itemIDList);
			result = log_delete;
			break;
		}
		
		return result;
	}
	
	void ReadResponseBody(InputStream is) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		this.setOperation(dis.readInt());
		this.setStartItemID(dis.readLong());
		this.setItemCount(dis.readLong());
		
		this.setSeedDNKey(dis.readUTF());
		this.setSeedDNHost(dis.readUTF());
		this.setSeedDNPort(dis.readInt());
		
		this.setSorted(dis.readBoolean());
		this.setVersion(dis.readInt());
		this.setPartitionID(dis.readInt());
		
		int size = dis.readInt();
		ArrayList<Long> list = new ArrayList<Long>();
		for (int i = 0;i<size;i++){
			list.add(dis.readLong());
		}
		itemIDList = list;
	}


	void WriteResponseBody(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(this.getOperation());
		dos.writeLong(this.getStartItemID());
		dos.writeLong(this.getItemCount());
		
		dos.writeUTF(this.getSeedDNKey());
		dos.writeUTF(this.getSeedDNHost());
		dos.writeInt(this.getSeedDNPort());
		
		dos.writeBoolean(this.isSorted());
		dos.writeInt(this.getVersion());
		dos.writeInt(this.getPartitionID());
		
		dos.writeInt(itemIDList.size());
		for (int i = 0;i<itemIDList.size();i++){
			dos.writeLong(itemIDList.get(i));
		}
	}

	public void addItemIDList(ArrayList<Long> idlist){
		itemIDList.addAll(idlist);
	}
	
	public void addItemID(long id){
		itemIDList.add(id);
	}
	
	public ArrayList<Long> listItemIDs(){
		return itemIDList;
	}

	public int getOperation() {
		return operation;
	}


	public void setOperation(int operation) {
		this.operation = operation;
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


	public String getSeedDNKey() {
		return seedDNKey;
	}


	public void setSeedDNKey(String seedDNKey) {
		this.seedDNKey = seedDNKey;
	}


	public boolean isSorted() {
		return sorted;
	}


	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}


	public int getVersion() {
		return version;
	}


	public void setVersion(int version) {
		this.version = version;
	}

	public int getPartitionID() {
		return partitionID;
	}

	public void setPartitionID(int partitionID) {
		this.partitionID = partitionID;
	}

	public String getSeedDNHost() {
		return seedDNHost;
	}

	public void setSeedDNHost(String seedDNHost) {
		this.seedDNHost = seedDNHost;
	}

	public int getSeedDNPort() {
		return seedDNPort;
	}

	public void setSeedDNPort(int seedDNPort) {
		this.seedDNPort = seedDNPort;
	}
}
