package com.cic.textengine.repository.datanode.type;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cic.textengine.repository.type.Writable;

/**
 * This class represents the appending point for a particular partition
 * 
 * @author denis.yu
 *
 */
public class PartitionAppendPoint implements Writable{
	int year, month;
	String siteID, forumID;
	long startItemID;
	int startItemIdx;
	int IDFIdx;
	int MAXItemCount;
	
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
	public long getStartItemID() {
		return startItemID;
	}
	public void setStartItemID(long startItemID) {
		this.startItemID = startItemID;
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
	public void read(InputStream is) throws IOException {
		// TODO Auto-generated method stub
		
	}
	public void write(OutputStream os) throws IOException {
		// TODO Auto-generated method stub
		
	}
	public int getMAXItemCount() {
		return MAXItemCount;
	}
	public void setMAXItemCount(int itemCount) {
		MAXItemCount = itemCount;
	}
}
