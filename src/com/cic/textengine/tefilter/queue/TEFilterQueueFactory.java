package com.cic.textengine.tefilter.queue;

import com.cic.textengine.tefilter.queue.impl.DataBaseQueueImpl;

public class TEFilterQueueFactory {

	private static TEFilterQueueFactory factory = null;
	
	private TEFilterQueueFactory() {
		
	}
	
	public static synchronized TEFilterQueueFactory getInstance() {
		if(factory == null) {
			factory = new TEFilterQueueFactory();
		}
		return factory;
	}
	
	public Queue getTEFilterQueue() {
		return new DataBaseQueueImpl();
	}
}
