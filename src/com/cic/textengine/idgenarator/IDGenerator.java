package com.cic.textengine.idgenarator;

public class IDGenerator {
	private long currentID = 0;
	private static IDGenerator gen = null;
	private IDGenerator(long start){
		currentID = start;
	}
	
	public static IDGenerator getInstance(long start){
		if (gen == null){
			gen = new IDGenerator(start);
		}
		return gen;
	}
	
	
	private long getNextID(){
		currentID ++;
		return currentID;
	}
	public static long getNext(){
		return gen.getNextID();
	}
}
