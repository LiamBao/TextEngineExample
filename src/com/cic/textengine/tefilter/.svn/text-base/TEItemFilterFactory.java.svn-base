package com.cic.textengine.tefilter;

import com.cic.textengine.tefilter.exception.TEItemFilterException;
import com.cic.textengine.tefilter.impl.BlogItemFilter;
import com.cic.textengine.tefilter.impl.DefaultFilter;

public class TEItemFilterFactory {
	
	private static TEItemFilterFactory factory = null;
	
	private TEItemFilterFactory() {
		
	}
	public static synchronized TEItemFilterFactory getInstance() {
		if(factory == null) {
			factory = new TEItemFilterFactory();
		}
		return factory;
	}
	
	public TEItemFilter getFilter(String filterName) throws TEItemFilterException {
		TEItemFilter filter = null;
		if(filterName.equals("BlogItemFilter"))
			filter = new BlogItemFilter();
		if(filterName.equals("DefaultFilter"))
			filter = new DefaultFilter();
		if(filter == null)
			throw new TEItemFilterException("Invalid filter name.");
		return filter;
	}

}
