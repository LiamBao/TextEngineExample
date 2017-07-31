package com.cic.textengine.tefilter;


import com.cic.textengine.tefilter.exception.TEItemFilterException;
import com.cic.textengine.type.TEItem;

public interface TEItemFilter {

	public boolean accept(TEItem item);
	public void close() throws TEItemFilterException;
	public void init() throws TEItemFilterException;
	public String getFilterName();
}