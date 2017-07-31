package com.cic.textengine.repository.type;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class PartitionKey {
	String siteID;
	String forumID;
	int year;
	int month;
	
	public PartitionKey(int year, int month, String siteid, String forumid){
		this.setYear(year);
		this.setMonth(month);
		this.setSiteID(siteid);
		this.setForumID(forumid);
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
	
	public String generateStringKey() {
		
		String key = null;
		
		try {
			key = new String(Hex.encodeHex(this.getSiteID().getBytes("utf-8")))
					+ "_"
					+ this.getYear()
					+ "_"
					+ new String(Hex.encodeHex(this.getForumID().getBytes("utf-8")))
					+ "_"
					+ this.getMonth();
		} catch (UnsupportedEncodingException e) {
			// ignore
		}
		return key;
	}
	
	public static PartitionKey decodeStringKey(String key) throws DecoderException{
		String[] keys = key.split("_", 4);

		int year = Integer.parseInt(keys[1]);
		int month = Integer.parseInt(keys[3]);
//		String siteid = new String(Hex.decodeHex(keys[0].toCharArray()));
//		String forumid = new String(Hex.decodeHex(keys[2].toCharArray()));
		String siteid = new String(Hex.decodeHex(keys[0].toCharArray()), Charset.forName("utf-8"));
		String forumid = new String(Hex.decodeHex(keys[2].toCharArray()),  Charset.forName("utf-8"));
		return new PartitionKey(year, month, siteid, forumid);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((forumID == null) ? 0 : forumID.hashCode());
		result = prime * result + month;
		result = prime * result + ((siteID == null) ? 0 : siteID.hashCode());
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final PartitionKey other = (PartitionKey) obj;
		if (forumID == null) {
			if (other.forumID != null)
				return false;
		} else if (!forumID.equals(other.forumID))
			return false;
		if (month != other.month)
			return false;
		if (siteID == null) {
			if (other.siteID != null)
				return false;
		} else if (!siteID.equals(other.siteID))
			return false;
		if (year != other.year)
			return false;
		return true;
	}
}
