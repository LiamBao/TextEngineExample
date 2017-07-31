package com.cic.textengine.tefilter.impl;

import com.cic.textengine.tefilter.TEItemFilter;
import com.cic.textengine.tefilter.exception.TEItemFilterException;
import com.cic.textengine.type.TEItem;

public class BlogItemFilter implements TEItemFilter{

	@Override
	public boolean accept(TEItem item) {
		if(item.getMeta().getSource().equalsIgnoreCase("blog"))
			return true;
		else
			return false;
	}

	@Override
	public void close() throws TEItemFilterException {
		
	}

	@Override
	public String getFilterName() {
		return "BlogItemFilter";
	}

	@Override
	public void init() throws TEItemFilterException {
		
	}

}
