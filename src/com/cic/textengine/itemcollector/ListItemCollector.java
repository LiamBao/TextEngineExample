package com.cic.textengine.itemcollector;

import java.util.ArrayList;
import java.util.List;

import com.cic.data.Item;

public class ListItemCollector implements ItemCollector {
	private List<Item> items = new ArrayList<Item>();

	public void collect(Item item) {
		items.add(item);
	}
	
	public List<Item> getItems(){
		return items;
	}

	public void clear() {
		items = new ArrayList<Item>();
	}
}
