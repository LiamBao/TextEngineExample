package com.cic.textengine.tefilter.impl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import com.cic.textengine.tefilter.TEItemFilter;
import com.cic.textengine.tefilter.exception.TEItemFilterException;
import com.cic.textengine.type.TEItem;

public class DefaultTEItemFilter implements TEItemFilter{

	private ArrayList<MatchCondition> conditionList = null;
	private static String conditionListFile = "DefaultFilterCondition.list";
	
	public DefaultTEItemFilter()
	{
		conditionList = new ArrayList<MatchCondition>();
	}
	
	@Override
	public boolean accept(TEItem item) {
		for(MatchCondition condition: conditionList)
		{
			if(condition.match(item))
			{
				return true;
			}
		}
		return false;
	}
	
	@Override
	public void close() throws TEItemFilterException{
		
	}
	
	@Override
	public void init() throws TEItemFilterException
	{
		try {
			FileReader fr = new FileReader(conditionListFile);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine()) != null)
			{
				String[] strArray = line.split(":");
				String source = strArray[0].trim();
				long siteid = Long.parseLong(strArray[1].trim());
				long threadID = Long.parseLong(strArray[2].trim());
				MatchCondition condition = new MatchCondition(source, siteid, threadID);
				this.conditionList.add(condition);
			}
			br.close();
			fr.close();
		} catch (FileNotFoundException e) {
			throw new TEItemFilterException(e);
		} catch (IOException e) {
			throw new TEItemFilterException(e);
		}
	}

	private static class MatchCondition
	{
		String source = null;
		long siteid = 0;
		long threadid = 0;
		
		public MatchCondition(String source, long siteid, long threadid)
		{
			this.source = source;
			this.siteid = siteid;
			this.threadid = threadid;
		}
		
		public boolean match(TEItem item)
		{
			if(!item.getMeta().getSource().equals(source))
				return false;
			if(item.getMeta().getSiteID()!=siteid)
				return false;
			if(item.getMeta().getThreadID()!=threadid)
				return false;
			return true;
		}
	}

	@Override
	public String getFilterName() {
		return "DefaultItemFilter";
	}

}