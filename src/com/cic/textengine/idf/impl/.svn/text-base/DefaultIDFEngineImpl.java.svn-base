package com.cic.textengine.idf.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.log4j.Logger;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.IDFReader;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;
import com.cic.textengine.type.TEItem;

/**
 * This is the default implementation of IDF Engine.
 * 
 * @author Denis.Yu
 *
 */
public class DefaultIDFEngineImpl implements IDFEngine {
	final static byte IDF_IMPL_VERSION_TAG = 0x01;

	final static int IO_BUFF_SIZE = 1024;
	
	final static int MAX_ITEM_NUMBER = 1048576; //Max 1M items.
	final static int ITEM_INDEX_LENGTH = 13;
	final static int IDF_HEADER_LENGTH = 6;
	final static long DATA_TRUNK_START_ADDRESS = MAX_ITEM_NUMBER * ITEM_INDEX_LENGTH + IDF_HEADER_LENGTH; //where the data trunk starts
	final static long INDEX_TRUNK_START_ADDRESS = IDF_HEADER_LENGTH;
	Logger m_logger = Logger.getLogger(IDFEngine.class);
	
	File m_file = null;
	
	int itemCount = 0;
	boolean IDFFull = false;
	
	/**
	 * Add items to the middle of the IDF file.
	 * 
	 * If this method fails because of IO exception. The item count of this IDF
	 * will be reset to start_idx - 1.
	 */
	public void addItems(ArrayList<TEItem> items, int start_idx)
			throws IDFEngineException, IOException {
		if (start_idx < 1){
			throw new IDFEngineException("Start index starts from 1.");
		}
		//check if the IDF will reach its item number limit
		if (start_idx - 1 + items.size() > MAX_ITEM_NUMBER){
			throw new IDFEngineException(
					"Can't add the new items because it exceeds the max number of items on IDF can contain. The max number of items on IDF can contain is "
							+ MAX_ITEM_NUMBER);
		}
		
		if (start_idx > this.getItemCount() + 1){//error
			throw new IDFEngineException("Can not add item from a start_idx which exceeds the current itemCount in IDF.");
		}else if (start_idx == this.getItemCount() + 1){
			
			m_logger.debug("Add item from the latest position, use append function.");
			this.appendItems(items);
		}else{//add from the middle
			
			//reset item count to start_idx - 1
			RandomAccessFile raf;
			raf = new RandomAccessFile(this.m_file,"rwd");
			raf.skipBytes(1);
			raf.writeInt(start_idx - 1);
			raf.close();
			this.itemCount = start_idx - 1;
			
			
			int count = 0;
			long address = 0;
			address = m_file.length();
			
			//first append the data to the tail of the file
			ArrayList<Integer> item_lengths = new ArrayList<Integer>();

			try {
				FileOutputStream fos = new FileOutputStream(this.m_file, true);

				int batch_size = 0;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);
				int buff_size_prev = 0;
				int buff_size = 0;
				int item_byte_length = 0;
				int current_item_index = start_idx;
				for (int i = 0;i<items.size();i++){
					dos.writeInt(current_item_index);//write the item index of this item
					current_item_index++;
					dos.writeLong(System.currentTimeMillis());	//write the time stamp
					items.get(i).write(dos);	
					buff_size = baos.size();
					item_byte_length = buff_size - buff_size_prev;
					buff_size_prev = buff_size;
					item_lengths.add(item_byte_length);
					batch_size++;
					
					if (batch_size > 512){
						//flush to file, empty output stream
						fos.write(baos.toByteArray());
						dos.close();
						baos.close();
						baos = new ByteArrayOutputStream();
						dos = new DataOutputStream(baos);
						batch_size = 0;
						buff_size_prev = 0;
					}
					
				}

				//flush to file, empty output stream
				if (batch_size > 0){
					fos.write(baos.toByteArray());
					dos.close();
					baos.close();
					baos = new ByteArrayOutputStream();
					dos = new DataOutputStream(baos);
				}
				fos.flush();
				fos.close();
			} catch (FileNotFoundException e1) {
				throw new IDFEngineException(e1);
			}
			
			//register the new items in the index trunk
			try {
				raf = new RandomAccessFile(this.m_file,"rwd");
				raf.seek(INDEX_TRUNK_START_ADDRESS + (start_idx-1) * ITEM_INDEX_LENGTH);
				count = start_idx - 1;
				for (int i = 0;i<item_lengths.size();i++){
					count++;
					raf.writeLong(address); //start address of the data
					raf.writeInt(item_lengths.get(i)); //length of the data
					raf.writeByte(0x00);//control byte
					address += item_lengths.get(i);
				}
				
				//update the count
				raf.seek(1);
				raf.writeInt(count);

				//if the IDF is full, mark it in the IDF control byte (1st bit)
				if (count >= this.getMaxItemCount()){
					raf.seek(5);
					byte idf_control = raf.readByte();
					idf_control = (byte)((int)idf_control | (int)0x80); //set the full tag = true
					raf.seek(5);
					raf.writeByte(idf_control);
					this.IDFFull = true;
				}else{
					raf.seek(5);
					byte idf_control = raf.readByte();
					idf_control = (byte)((int)idf_control & (int)0x7F);	//set the full tag = false
					raf.seek(5);
					raf.writeByte(idf_control);
					this.IDFFull = false;
				}
				
				raf.close();
				
				this.itemCount = count;
			} catch (FileNotFoundException e) {
				throw new IDFEngineException(e);
			} catch (IOException e) {
				throw new IDFEngineException(e);
			}					
		}
	}
	
	public synchronized int appendItems(ArrayList<TEItem> items)
	throws IDFEngineException, IOException{
		//check if the IDF will reach its item number limit
		
		if (this.isFull() || (this.itemCount + items.size() > MAX_ITEM_NUMBER)){
			throw new IDFEngineException(
					"Can't add the new items because it exceeds the max number of items on IDF can contain. The max number of items on IDF can contain is "
							+ MAX_ITEM_NUMBER);
		}
		
		int count = 0;

		long address = 0;
		address = m_file.length();
		//first append the data to the tail of the file
		ArrayList<Integer> item_lengths = new ArrayList<Integer>();
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(this.m_file,true);
			int batch_size = 0;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			int buff_size_prev = 0;
			int buff_size = 0;
			int item_byte_length = 0;
			int current_item_index = this.getItemCount() + 1;
			for (int i = 0;i<items.size();i++){
				dos.writeInt(current_item_index);
				current_item_index++;
				dos.writeLong(System.currentTimeMillis());
				items.get(i).write(dos);
				
				buff_size = baos.size();
				item_byte_length = buff_size - buff_size_prev;
				buff_size_prev = buff_size;
				item_lengths.add(item_byte_length);
				batch_size++;
				
				if (batch_size > 512){
					//flush to file, empty output stream
					fos.write(baos.toByteArray());
					dos.close();
					baos.close();
					baos = new ByteArrayOutputStream();
					dos = new DataOutputStream(baos);
					batch_size = 0;
					buff_size_prev = 0;
				}
				
			}

			//flush to file, empty output stream
			if (batch_size > 0){
				fos.write(baos.toByteArray());
				dos.close();
				baos.close();
			}
			fos.flush();
			fos.close();
		} catch (FileNotFoundException e1) {
			throw new IDFEngineException(e1);
		}
		//register the new items
		RandomAccessFile raf;
		int startID = 0;
		try {
			raf = new RandomAccessFile(this.m_file,"rwd");
			count = this.getItemCount();
			startID = count+1;
			
			raf.seek(INDEX_TRUNK_START_ADDRESS + count * ITEM_INDEX_LENGTH);
			
			for (int i = 0;i<item_lengths.size();i++){
				count++;
				raf.writeLong(address); //start address of the data
				raf.writeInt(item_lengths.get(i)); //length of the data
				raf.writeByte(0x00);//control byte
				address += item_lengths.get(i);
			}
			
			//update the count
			
			raf.seek(1);
			raf.writeInt(count);
			
			//if the IDF is full, mark it in the IDF control byte (1st bit)
			if (count >= this.getMaxItemCount()){
				raf.seek(5);
				byte idf_control = raf.readByte();
				idf_control = (byte)((int)idf_control | (int)0x80);
				raf.seek(5);
				raf.writeByte(idf_control);
			}
			raf.close();
			
			this.itemCount = count;
			
			return startID;
		} catch (FileNotFoundException e) {
			throw new IDFEngineException(e);
		}
	}
	
	public synchronized void createIDF()
	throws IDFEngineException{
		File file = this.getFile();
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file, false);
		} catch (FileNotFoundException e) {
			throw new IDFEngineException(e);
		}
		
		byte[] buff = new byte[1024];
		for (int i = 0;i<1024;i++){
			buff[i] = 0;
		}
		buff[0] = IDF_IMPL_VERSION_TAG;
		
		try {
			fos.write(buff,0,6);//write IDF header
			
			buff[0] = 0x00;
			//write IDF index area
			for (int i = 0;i<1024 * 13;i++){
				fos.write(buff,0,1024);
			}
			fos.flush();
			fos.close();
		} catch (IOException e) {
			throw new IDFEngineException(e);
		}
		
		this.itemCount = 0;
	}
	
	synchronized void emptyIDF()
	throws IDFEngineException{
		File file = this.m_file;
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file,"rwd");
			raf.skipBytes(1); //skip the IDF version tag
			raf.writeInt(0); //reset itemcount = 0
			raf.writeByte(0);//empty idf control byte
			raf.close();
			
			this.itemCount = 0;
		} catch (FileNotFoundException e) {
			throw new IDFEngineException(e);
		} catch (IOException e) {
			throw new IDFEngineException(e);
		}
	}
	
	public File getFile() {
		return m_file;
	}
	
	public IDFReader getIDFReader() throws IOException {
		return getIDFReader(1, false);
	}

	public IDFReader getIDFReader(int startItemIndex, boolean includeDeletedItems)
	throws IOException {
		IDFReader reader = new DefaultIDFReaderImpl(this, startItemIndex, includeDeletedItems);
		return reader;
	}
	
	/**
	 * Get the absolute item index address in IDF according to a particular itemid
	 * 
	 * @param itemID
	 * @return
	 */
	long getIdxAddress(int itemIndex){
		return (itemIndex - 1) * 13 + 6;
	}
	
	/**
	 * Get a item instance according to item index. 
	 */
	public TEItem getItem(int itemIndex)
	throws IDFEngineException{
		DataInputStream dis;
		
		try {
			dis = new DataInputStream(new FileInputStream(this.m_file));
		} catch (FileNotFoundException e) {
			throw new IDFEngineException(e);
		}
		
		if (itemIndex > this.getItemCount()){
			return null;
		}
		
		int count = 0;

		TEItem item = null;
		try {
			
			long current_addr = 0;
			dis.skip(1);	
			current_addr += 1;
			count = dis.readInt();
			current_addr += 4;
			dis.skip(1);
			current_addr += 1;
			byte control_byte = 0x00;
			long address = 0;
			if (count >= itemIndex){
				dis.skip((itemIndex-1) * ITEM_INDEX_LENGTH);
				current_addr += (itemIndex-1) * ITEM_INDEX_LENGTH;
				address = dis.readLong();
				current_addr += 8;
				dis.skip(4);
				current_addr += 4;
				control_byte = dis.readByte();
				current_addr += 1;
				if ((byte)(control_byte & 0x80) != (byte)0x80){//ignore the deleted item;
					dis.skip(address - current_addr);//position the file pointer to the start address of the data trunk for the item
					
					dis.skipBytes(4 + 8);//skip the index and ts
					item = new TEItem();
					item.readFields(dis);
				}
			}
			return item;
		} catch (IOException e) {
			throw new IDFEngineException(e);
		} finally{
			try {
				dis.close();
			} catch (IOException e) {
				//ignore
			}
		}
	}

	public int getItemCount(){
		return this.itemCount;
	}

	/**
	 * Only undeleted items will be returned.
	 */
	public ArrayList<TEItem> getItems(ArrayList<Integer> index_list, boolean sorted)
	throws IDFEngineException{
		ArrayList<TEItem> result = new ArrayList<TEItem>();
		
		ArrayList<Integer> id_list = null;
		if (!sorted){
			id_list = new ArrayList<Integer>(index_list);
			Collections.sort(id_list);
		}else{
			id_list = index_list;
		}
		
		DataInputStream dis = null;
		try {
			dis = new DataInputStream(new FileInputStream(this.m_file));
		} catch (FileNotFoundException e) {
			throw new IDFEngineException(e);
		}
		int count = 0;

		TEItem item = null;
		try {
			
			long current_addr = 0;
			count = this.getItemCount();
			dis.skip(6);
			current_addr += 6;
		
			ArrayList<Long> address_list = new ArrayList<Long>();
			ArrayList<Integer> length_list = new ArrayList<Integer>();
			
			long address = 0;
			long data_address = 0;
			int length = 0;
			int itemIndex;
			byte control_byte = 0x00;
			//find the data address for all those items
			for (int i = 0;i<id_list.size();i++){
				itemIndex = id_list.get(i);
				
				if (count < itemIndex || itemIndex <= 0){
					break;
				}
				
				address = getIdxAddress(itemIndex);
				dis.skip(address - current_addr);
				current_addr = address;
				data_address = dis.readLong();
				length = dis.readInt();
				control_byte = dis.readByte();
				current_addr += 13;
				if ((byte)(control_byte & 0x80) != (byte)0x80){//ignore the deleted item;
					address_list.add(data_address);
					length_list.add(length);
				}
			}
			
			//populate the item data
			for (int i = 0;i<address_list.size();i++){
				data_address = address_list.get(i);
				dis.skip(data_address - current_addr);
				dis.skipBytes(12);//skip the index and timestamp
				
				item = new TEItem();
				item.readFields(dis);
				current_addr = data_address + length_list.get(i);
				
				result.add(item);
			}
			return result;
		} catch (IOException e) {
			throw new IDFEngineException(e);
		} finally{
			try {
				dis.close();
			} catch (IOException e) {
				//ignore
			}
		}
	}

	public int getMaxItemCount() {
		return MAX_ITEM_NUMBER;
	}

	public synchronized void init(File file) 
	 throws IDFEngineInitException,IDFEngineException{
		this.m_file = file;
		if (!(m_file.exists() && m_file.isFile())){
			if (m_logger.isDebugEnabled()){
				m_logger.debug("Can't find IDF physical file ["
						+ file.getAbsolutePath()
						+ "], create an empty one.");
			}
			createIDF();
			this.itemCount = 0;
			this.IDFFull = false;
		}else{
			if (this.m_file.length() < DATA_TRUNK_START_ADDRESS){
				if (m_logger.isDebugEnabled()){
					m_logger.debug("Find 0 length file ["
							+ file.getAbsolutePath()
							+ "], format IDF.");
				}
				createIDF();
				this.itemCount = 0;
				this.IDFFull = false;
			}else{
				readItemCount();
				if (m_logger.isDebugEnabled()){
					m_logger.debug("IDF file ["
							+ file.getAbsolutePath()
							+ "] is inited, totally " + itemCount + " found.");
				}
			}
		}
	}

	public boolean isFull() {
		return IDFFull;
	}

	void readItemCount()
	throws IDFEngineException{
		DataInputStream dis;
		try {
			dis = new DataInputStream(new FileInputStream(this.m_file));
		} catch (FileNotFoundException e) {
			throw new IDFEngineException(e);
		}
		int count = 0;
		
		byte idf_control;
		try {
			byte idf_engine_tag = dis.readByte();
			if (idf_engine_tag != IDF_IMPL_VERSION_TAG)
				throw new IDFEngineException("Illegal IDF Engine File. This engine is not for the file specified.");

			count = dis.readInt();
			idf_control = dis.readByte();
			
			if ((idf_control & 0x80) == 0x80){
				this.IDFFull = true;
			}else{
				this.IDFFull = false;
			}
			
		} catch (IOException e) {
			throw new IDFEngineException(e);
		} finally{
			try {
				if (dis != null)
					dis.close();
			} catch (IOException e) {
				//ignore
			}
		}
		
		
		this.itemCount = count;
	}

	
	public void deleteItems(ArrayList<Integer> index_list, boolean sorted)
			throws IDFEngineException {
		ArrayList<Integer> id_list = null;
		if (!sorted){
			id_list = new ArrayList<Integer>(index_list);
			Collections.sort(id_list);
		}else{
			id_list = index_list;
		}
		
		RandomAccessFile raf = null;

		try {
			raf = new RandomAccessFile(m_file,"rwd");
		} catch (FileNotFoundException e) {
			throw new IDFEngineException(e);
		}

		int count = 0;

		try {
			count = this.getItemCount();
			int itemIndex = 0;
			long address;
			byte control_byte = 0x00;

			//find the data address for all those items
			for (int i = 0;i<id_list.size();i++){
				
				itemIndex = id_list.get(i);
				
				if (count < itemIndex){
					break;
				}
				
				address = getIdxAddress(itemIndex);
				raf.seek(address + 12);
				control_byte = raf.readByte();
				control_byte = (byte)(control_byte | 0x80);
				raf.seek(address + 12);
				raf.writeByte(control_byte);
			}
			
		} catch (IOException e) {
			throw new IDFEngineException(e);
		} finally{
			try {
				raf.close();
			} catch (IOException e) {
				//ignore
			}
		}
	}

	public void destroy() {
		m_file.delete();
		m_file = null;
	}

}
