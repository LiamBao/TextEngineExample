package com.cic.textengine.repository.type;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.cic.textengine.type.TEItem;

public class ItemKey implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1591533971643111283L;
	
	private String source = null;
	private String siteid = null;
	private String forumid = null;
	private int year = 0;
	private int month = 0;
	private long itemid = 0;
	private String partitionkey = null;

	public ItemKey(String source, String siteid, String forumid, int year, int month, long itemid)
	{
		this.source = source;
		this.siteid = siteid;
		this.forumid = forumid;
		this.year = year;
		this.month = month;
		this.itemid = itemid;
		this.partitionkey = (new PartitionKey(year, month, source+siteid, forumid)).generateStringKey();
	}
	
	public ItemKey(TEItem item)
	{
		this.source = item.getMeta().getSource();
		this.siteid = Long.toString(item.getMeta().getSiteID());
		this.forumid = item.getMeta().getForumID();
		this.year = item.getMeta().getYearOfPost();
		this.month = item.getMeta().getMonthOfPost();
		this.itemid = item.getMeta().getItemID();
		this.partitionkey = (new PartitionKey(year, month, source+siteid, forumid)).generateStringKey();
	}
	public String getSource()
	{
		return this.source;
	}
	public String getSiteID()
	{
		return this.siteid;
	}
	public String getForumID()
	{
		return this.forumid;
	}
	public int getYear()
	{
		return this.year;
	}
	public int getMonth()
	{
		return this.month;
	}
	public long getItemID()
	{
		return this.itemid;
	}
	public String getPartitionKey()
	{
		return this.partitionkey;
	}
	// ItemKey format: source_year_siteid_month_forumid_itemid
	
	public String generateKey()
	{
		String key = null;
		try {
			key = new String(Hex.encodeHex(this.getSource().getBytes("utf-8")))
			+ "_"
			+ this.getYear()
			+ "_"
			+ new String(Hex.encodeHex(this.getSiteID().getBytes("utf-8")))
			+ "_"
			+ this.getMonth()
			+ "_"
			+ new String(Hex.encodeHex(this.getForumID().getBytes("utf-8")))
			+ "_"
			+ this.getItemID();
		} catch (UnsupportedEncodingException e)
		{
			//ignore
		}
		
		return key;
	}
	
	public static ItemKey decodeKey(String key) throws DecoderException
	{
		String[] keys = key.split("_", 6);
		int year = Integer.parseInt(keys[1]);
		int month = Integer.parseInt(keys[3]);
		long itemid = Long.parseLong(keys[5]);
//		String source = new String(Hex.decodeHex(keys[0].toCharArray()));
//		String siteid = new String(Hex.decodeHex(keys[2].toCharArray()));
//		String forumid = new String(Hex.decodeHex(keys[4].toCharArray()));
		String source = new String(Hex.decodeHex(keys[0].toCharArray()), Charset.forName("utf-8"));
		String siteid = new String(Hex.decodeHex(keys[2].toCharArray()), Charset.forName("utf-8"));
		String forumid = new String(Hex.decodeHex(keys[4].toCharArray()), Charset.forName("utf-8"));
		ItemKey itemkey = new ItemKey(source, siteid, forumid, year, month, itemid);
		return itemkey;
	}
}
