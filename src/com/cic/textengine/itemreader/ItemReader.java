package com.cic.textengine.itemreader;

import com.cic.data.Item;

public interface ItemReader {
	public Item getItem(); 
	public boolean next() throws Exception;
	public void close() throws Exception;
}
