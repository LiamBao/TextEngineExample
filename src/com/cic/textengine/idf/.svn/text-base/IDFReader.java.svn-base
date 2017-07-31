package com.cic.textengine.idf;

import java.io.IOException;

import com.cic.textengine.type.TEItem;

public interface IDFReader {

	/**
	 * Move to the next item.
	 * @return	Return false if there's no next item available.
	 * 
	 * @throws IOException
	 */
	public abstract boolean next() throws IOException;

	/**
	 * Get the current item.
	 * 
	 * @return
	 */
	public abstract TEItem getItem();

	/**
	 * Close the reader, release all resources.
	 * @throws IOException
	 */
	public abstract void close() throws IOException;

}