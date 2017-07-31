package com.cic.textengine.repository.datanode.repository;

import java.io.File;
import java.util.ArrayList;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;

public interface RepositoryEngine {

	public IDFEngine getIDFEngine(int year, int month, String siteid, String forumid, int idx)
		throws RepositoryEngineException;

	public void init(File repPath)	throws RepositoryEngineException;
	
	/**
	 * Clean all IDF which are managed by this repository engine
	 * 
	 * @return
	 */
	public boolean clean();
	
	public PartitionEnumerator getPartitionEnumerator(int year, int month,
			String siteid, String forumid, long startItemID,
			boolean includeDeletedItems) throws RepositoryEngineException;

	public PartitionEnumerator getPartitionEnumerator(int year, int month,
			String siteid, String forumid) throws RepositoryEngineException;
	
	public PartitionWriter getPartitionWriter(int year, 
			int month,
			String siteid, String forumid,
			long startItemID) throws RepositoryEngineException;
	
	public PartitionSearcher getPartitionSearcher(int year, 
			int month,
			String siteid, String forumid) throws RepositoryEngineException;

	/**
	 * Delete a series of items according to item id in the partition.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param itemids
	 */
	public void deleteItems(int year, int month, String siteid, String forumid,
			ArrayList<Long> itemids, boolean sorted) throws RepositoryEngineException;
}
