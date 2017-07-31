package com.cic.textengine.repository.datanode.repository.impl;

import java.io.IOException;
import java.util.ArrayList;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

public class DefaultPartitionWriterImpl implements PartitionWriter {
	static final int BUFF_FLUSH_THRESHHOLD = 4000;
	int currentIDFIndex = 0;
	int currentStartItemIndex = 0;
	
	IDFEngine currentIDFEngine = null;
	
	long startItemID = 1;
	long currentItemID = 1;
	
	ArrayList<TEItem> m_buff = new ArrayList<TEItem>();
	
	int year, month;
	String siteid, forumid;
	
	RepositoryEngine m_repositoryEngine = null;
	
	DefaultPartitionWriterImpl(RepositoryEngine repository_engine, 
			int year, int month,
			String siteid, String forumid,
			long startItemID) 
	throws RepositoryEngineException{
		int idfidx = 0;
		long count = startItemID;
		IDFEngine idfengine = null;
		idfengine = repository_engine.getIDFEngine(year, month, siteid, forumid, idfidx);
		while(count > idfengine.getMaxItemCount()){
			if (idfengine.getItemCount() < idfengine.getMaxItemCount()){
				RepositoryEngineException exception = new RepositoryEngineException(
					"IDFEngine is not supposed to be full.");
				exception.setParititonKey(year, month, siteid, forumid);

				throw exception;
			}
			count = count - idfengine.getMaxItemCount();
			idfidx++;
			idfengine = repository_engine.getIDFEngine(year, month, siteid, forumid, idfidx);
		}

		currentIDFIndex = idfidx;
		currentStartItemIndex = (int)count;
		currentIDFEngine = idfengine;
		this.startItemID = startItemID;
		this.currentItemID = startItemID;
		
		if (currentStartItemIndex > idfengine.getItemCount() + 1){
			RepositoryEngineException exception = new RepositoryEngineException(
					"Can not add items to partition from the position " +
					"which exceed the current item count in the partition." +
					"[startItemIdx:" + currentStartItemIndex +
					",IDFItemCount:" + idfengine.getItemCount());
			exception.setParititonKey(year, month, siteid, forumid);
			throw exception;
			
		}
		
		this.year = year;
		this.month = month;
		this.siteid = siteid;
		this.forumid = forumid;
		this.m_repositoryEngine = repository_engine;
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.PartitionWriter#close()
	 */
	public void close() throws RepositoryEngineException{
		flush();
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.PartitionWriter#flush()
	 */
	public void flush() throws RepositoryEngineException{
		try {
			flushBuffer();
		} catch (IDFEngineException e) {
			throw new RepositoryEngineException(e);
		} catch (IOException e) {
			throw new RepositoryEngineException(e);
		}
	}
	
	void flushBuffer() throws IDFEngineException, IOException, RepositoryEngineException{
		if (m_buff.size()<=0){
			return;
		}
		currentIDFEngine.addItems(m_buff, currentStartItemIndex);
		currentStartItemIndex += m_buff.size();
		
		if (currentStartItemIndex > currentIDFEngine.getMaxItemCount()){
			currentIDFIndex++;
			currentIDFEngine = m_repositoryEngine.getIDFEngine(year, month, siteid, forumid, currentIDFIndex);
			currentStartItemIndex = 1;
		}
		m_buff.clear();
	}
	
	public long getStartItemID() {
		return startItemID;
	}

	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.PartitionWriter#writeItem(com.cic.textengine.type.TEItem)
	 */
	public synchronized void writeItem(TEItem item) 
	throws RepositoryEngineException{
		item.getMeta().setItemID(this.currentItemID);
		this.currentItemID++;
		m_buff.add(item);
		if (m_buff.size() > BUFF_FLUSH_THRESHHOLD || 
				m_buff.size() + currentStartItemIndex -1 >= currentIDFEngine.getMaxItemCount()) {
			try {
				flushBuffer();
			} catch (IDFEngineException e) {
				RepositoryEngineException exception = 
					new RepositoryEngineException(e);
				exception.setParititonKey(year, month, siteid, forumid);
				throw exception;
			} catch (IOException e) {
				RepositoryEngineException exception = 
					new RepositoryEngineException(e);
				exception.setParititonKey(year, month, siteid, forumid);
				throw exception;
			}
		}
	}
}
