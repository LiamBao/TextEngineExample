package com.cic.textengine.type;

import java.io.Serializable;
import java.util.ArrayList;

import com.cic.textengine.posttrend.PostTrend;
import com.cic.textengine.repository.type.PartitionKey;

public class PartitionDeleteInfo implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<Long> idlist = null;
	private int posttrend = 0;
	private String key = null;
	private boolean delete_whole_partition = false;
	
	public PartitionDeleteInfo(String key){
		this.idlist = new ArrayList<Long>();
		this.key = key;
	}
	public void addItemID(long id)
	{
		if(this.idlist.contains(id))
			return;
		this.idlist.add(id);
	}
	public void setPostTrend() throws Exception
	{
		PostTrend posttrend = new PostTrend();
		PartitionKey parKey = PartitionKey.decodeStringKey(key);
		int count = posttrend.getTrend(parKey);
		this.posttrend = count - idlist.size();
		if(this.posttrend < 0)
		{
			this.posttrend = 0;
//			throw new Exception("There are more items found.");
		}
	}
	public int getposttrend()
	{
		return this.posttrend;
	}
	public ArrayList<Long> getIDList()
	{
		return this.idlist;
	}
	public boolean containID(long id)
	{
		return this.idlist.contains(id);
	}
	public String getParkey()
	{
		return this.key;
	}
	public void setDeleteWholePartition(boolean delete)
	{
		this.delete_whole_partition = delete;
	}
	public boolean getDeleteWholePartition()
	{
		return this.delete_whole_partition;
	}
	
}
