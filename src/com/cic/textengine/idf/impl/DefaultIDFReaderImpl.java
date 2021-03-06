package com.cic.textengine.idf.impl;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.IDFReader;
import com.cic.textengine.type.TEItem;

public class DefaultIDFReaderImpl implements IDFReader {
	IDFEngine m_engine = null;
	DataInputStream m_ISItemID = null;
	DataInputStream m_ISItemData = null;
	
	int m_itemCount = 0;
	int m_itemDataPointer = 0; //pointer that points to the current item.
	
	long m_currentDataTrunkAddress = 0;
	
	TEItem m_currentItem = null;
	
	boolean includeDeletedItems = false;
	
	boolean endOfReader = false;
	
	DefaultIDFReaderImpl(IDFEngine engine, int startItemIndex, boolean includeDeletedItems) 
	throws IOException{
			
		
		m_engine = engine;
		File file = m_engine.getFile();
		m_ISItemID = new DataInputStream(new FileInputStream(file));
		m_ISItemData = new DataInputStream(new FileInputStream(file));
		
		m_itemCount = engine.getItemCount();
		if (startItemIndex < 1 ){
			startItemIndex = 1;
		}else if (startItemIndex > m_itemCount){
			endOfReader = true;			
		}
		
		m_ISItemID.skip(6 + (startItemIndex - 1) * 13);
		
		m_currentDataTrunkAddress = 0;
		
		this.m_itemDataPointer = startItemIndex - 1;
		
		this.includeDeletedItems = includeDeletedItems;
		
		
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.idf.impl.IDFReader#next()
	 */
	public boolean next() throws IOException{
		if (this.endOfReader)
			return false;
		
		m_itemDataPointer++;
		if (m_itemDataPointer > m_itemCount){
			return false;
		}
		
		long address = m_ISItemID.readLong();
		int length = m_ISItemID.readInt();
		byte control_byte = m_ISItemID.readByte();
		
		if ((byte)(control_byte & 0x80) == 0x00 || this.includeDeletedItems){

			m_ISItemData.skip(address - m_currentDataTrunkAddress);
			m_ISItemData.skip(12);

			TEItem item = new TEItem();
			item.readFields(m_ISItemData);
			m_currentDataTrunkAddress = address + length;
		
			m_currentItem = item;
			
			return true;
		}else{
			return next();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.idf.impl.IDFReader#getItem()
	 */
	public TEItem getItem(){
		return m_currentItem;
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.idf.impl.IDFReader#close()
	 */
	public void close() 
	throws IOException{
		m_ISItemID.close();
		m_ISItemData.close();
	}
	
	public void finalize(){
		try {
			this.close();
		} catch (IOException e) {
			//ignore
		}
	}
}
