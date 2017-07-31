package com.cic.textengine.repository.datanode.repository.impl;

import java.io.IOException;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.IDFReader;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

/**
 * Enumerate all items in the partition.
 * @author denis.yu
 */
public class DefaultPartitionEnumeratorImpl implements PartitionEnumerator {
	int currentIDFIndex = 0;
	
	IDFEngine currentIDFEngine = null;
	IDFReader currentIDFReader = null;
	boolean lastIDF = false;
	boolean includeDeletedItems = false;
	
	int year, month;
	String siteid, forumid;
	
	RepositoryEngine m_repositoryEngine = null;
	long startItemID = 1;
	
	boolean endOfReader = false;

	DefaultPartitionEnumeratorImpl(RepositoryEngine repository_engine,
			int year, int month, String siteid, String forumid)
			throws RepositoryEngineException {
		this(repository_engine, year, month, siteid, forumid,1, false);
	}
	
	DefaultPartitionEnumeratorImpl(RepositoryEngine repository_engine,
			int year, int month, String siteid, String forumid,long startItemID,
			boolean includeDeletedItems) throws RepositoryEngineException {
		this.includeDeletedItems = includeDeletedItems;
		this.startItemID = startItemID;
		this.year = year;
		this.month = month;
		this.siteid = siteid;
		this.forumid = forumid;
		this.m_repositoryEngine = repository_engine;		
	
		int idfidx = 0;
		IDFEngine idfengine = null;
		idfengine = repository_engine.getIDFEngine(year, month, siteid, forumid, idfidx);
		long item_count = 0;
		item_count += idfengine.getItemCount();
		
		while (startItemID > item_count && idfengine.getItemCount() > 0){
			idfidx++;
			idfengine = repository_engine.getIDFEngine(year, month, siteid, forumid, idfidx);
			item_count += idfengine.getItemCount();
		}
		
		if (idfengine.getItemCount()<=0){
			endOfReader = true;
		}
		
		if (idfengine.getItemCount() < idfengine.getMaxItemCount()){
			this.lastIDF = true;
		}
		
		
		
		
		int startItemIndex = 1;
		startItemIndex = (int) (startItemID - (item_count - idfengine
				.getItemCount()));
		try {
			IDFReader idfreader = idfengine.getIDFReader(startItemIndex,
					includeDeletedItems);
			this.currentIDFReader = idfreader;
		} catch (IOException e) {
			throw new RepositoryEngineException(e);
		}

		this.currentIDFIndex = idfidx;
		this.currentIDFEngine = idfengine;
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.impl.PartitionEnumerator#next()
	 */
	public boolean next() throws IOException, RepositoryEngineException{
		if (endOfReader)
			return false;
		
		if (this.currentIDFReader.next()){
			return true;
		}else{
			if (lastIDF){
				return false;
			}else{
				currentIDFReader.close();
				
				this.currentIDFIndex++;
				currentIDFEngine = m_repositoryEngine.getIDFEngine(year, month, siteid, forumid, currentIDFIndex);
				currentIDFReader = currentIDFEngine.getIDFReader();
				if (currentIDFEngine.getItemCount() < currentIDFEngine.getMaxItemCount()){
					lastIDF = true;
				}
				return currentIDFReader.next();
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.impl.PartitionEnumerator#getItem()
	 */
	public TEItem getItem(){
		return currentIDFReader.getItem();
	}
		
	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.impl.PartitionEnumerator#close()
	 */
	public void close() throws IOException{
		if (currentIDFReader!=null){
			currentIDFReader.close();
			currentIDFReader = null;
		}
	}
	
	public void finalize() throws Throwable{
		try {
			this.close();
		} catch (IOException e) {
			//ignore
		}
		super.finalize();
	}
}
