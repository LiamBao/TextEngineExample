package com.cic.textengine.idf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;
import com.cic.textengine.type.TEItem;

public interface IDFEngine {
	
	/**
	 * Retrieve the IDF file object.
	 * @return
	 */
	public File getFile();
	
	/**
	 * Check the IDF, if IDF doesn't exists, create an empty one.
	 * @param file
	 * @throws IDFEngineInitException
	 */
	public void init(File file) 
		throws IDFEngineInitException,IDFEngineException;

	/**
	 * append a series of items to IDF at the end. 
	 * @param items
	 * @return	Return the start ID of the items that have been successfully
	 * 			added into IDF. The following IDs of all items will be a series
	 * 			of numbers which are increased by 1
	 * @throws IDFEngineException
	 */
	public int appendItems(ArrayList<TEItem> items)
	throws IDFEngineException, IOException;
	
	
	public void addItems(ArrayList<TEItem> items, int start_idx)
	throws IDFEngineException, IOException;
	
	/**
	 * Delete items from the IDF.
	 * @param items
	 * @param start_idx
	 * @throws IDFEngineException
	 * @throws IOException
	 */
	public void deleteItems(ArrayList<Integer> index_list, boolean sorted) throws IDFEngineException;
	
	/**
	 * Return the total items that stored in the IDF, INCLUDE THOSE DELETED ITEMS.
	 * @return
	 */
	public int getItemCount();

	/**
	 * Get items info according to a list of itemids
	 * If the item id list is pre-sorted in ascending order, the performance will be faster.
	 * 
	 * @param ids
	 * @param sorted
	 * @return
	 * @throws IDFEngineException
	 */
	public ArrayList<TEItem> getItems(ArrayList<Integer> index_list, boolean sorted) throws IDFEngineException;
	public TEItem getItem(int itemIndex) throws IDFEngineException;
	
	/**
	 * Get the IDFReader for stream reading an IDF file.
	 * @return
	 * @throws IOException
	 */
	public IDFReader getIDFReader() throws IOException;

	/**
	 * Get the IDFReader for stream reading an IDF file.
	 * @return
	 * @throws IOException
	 */
	public IDFReader getIDFReader(int startItemIndex,
			boolean includeDeletedItems) throws IOException;

	/**
	 * Create an empty IDF, if the file already exists, the original file will be overwritten.
	 * @param file
	 * @throws IDFEngineException
	 */
	public void createIDF() throws IDFEngineException;
	
	/**
	 * Get the max number of items that IDF can support
	 * @return
	 */
	public int getMaxItemCount();
	
	/**
	 * Is the IDF already full of items any no way to add more items
	 * @return
	 */
	public boolean isFull();

	/**
	 * Destroy the IDF file.
	 */
	public void destroy();
}