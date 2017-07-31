package com.cic.textengine.tefilter.queue;

import com.cic.textengine.tefilter.queue.exception.TEFilterQueueException;
import com.cic.textengine.type.TEItem;

public interface Queue {
	
	public void init() throws TEFilterQueueException;
	public void put(String filterName, String itemkey) throws TEFilterQueueException;
	public TEItem get(String filterName) throws TEFilterQueueException;
	public void close() throws TEFilterQueueException;
	
}
