package com.cic.textengine.repository.namenode.client.type;

import java.util.ArrayList;

public class PartitionOperation {
	int operation = 0;
	int year;
	int month;
	String siteID;
	String forumID;
	int version = 0;
	
	//for partition add operation
	long startItemID = 0;
	long itemCount = 0;
	String seedDNHost = null;
	int seedDNPort = 0;
	
	//for partition delete operation
	boolean deleteItemIDListSorted = false;
	ArrayList<Long> deleteItemIDList = new ArrayList<Long>();
	
	public ArrayList<Long> listDeletedItemIDList(){
		return deleteItemIDList;
	}
	
	public void addDeletedItemIDList(ArrayList<Long> list){
		deleteItemIDList.addAll(list);
	}
	
	public int getOperation() {
		return operation;
	}
	public void setOperation(int operation) {
		this.operation = operation;
	}
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public String getSiteID() {
		return siteID;
	}
	public void setSiteID(String siteID) {
		this.siteID = siteID;
	}
	public String getForumID() {
		return forumID;
	}
	public void setForumID(String forumID) {
		this.forumID = forumID;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
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
	public boolean isDeleteItemIDListSorted() {
		return deleteItemIDListSorted;
	}
	public void setDeleteItemIDListSorted(boolean deleteItemIDListSorted) {
		this.deleteItemIDListSorted = deleteItemIDListSorted;
	}
}
