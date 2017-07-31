package com.cic.textengine.itemcollector;

import com.cic.data.Item;

public interface ItemCollector {
	public void collect(Item item);
	public void clear();
}
