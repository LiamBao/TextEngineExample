package com.cic.textengine.repository.datanode.repository;

import java.io.IOException;

import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

/**
 * Partition writer is to write a series items to an partition
 * @author denis.yu
 *
 */
public interface PartitionWriter {

	/**
	 * Write an item to the partition.
	 * @param item
	 * @throws IDFEngineException
	 * @throws IOException
	 * @throws RepositoryEngineException
	 */
	public abstract void writeItem(TEItem item) throws RepositoryEngineException;

	/**
	 * Flush the unwritten items to the partition.
	 * @throws IDFEngineException
	 * @throws IOException
	 * @throws RepositoryEngineException
	 */
	public abstract void flush() throws RepositoryEngineException;

	/**
	 * Close the writer
	 * @throws IDFEngineException
	 * @throws IOException
	 * @throws RepositoryEngineException
	 */
	public abstract void close() 
	throws RepositoryEngineException;

	/**
	 * Get the start item id of this writer.
	 * @return
	 */
	public long getStartItemID();
}