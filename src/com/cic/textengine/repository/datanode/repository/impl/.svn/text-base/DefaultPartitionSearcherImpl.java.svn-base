package com.cic.textengine.repository.datanode.repository.impl;

import java.util.ArrayList;
import java.util.Collections;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.repository.datanode.repository.PartitionSearcher;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

class DefaultPartitionSearcherImpl implements PartitionSearcher {
	int year, month;
	String siteid, forumid;
	RepositoryEngine m_repositoryEngine = null;

	DefaultPartitionSearcherImpl(RepositoryEngine repository_engine, 
			int year, int month,
			String siteid, String forumid) 
	throws RepositoryEngineException{
		
		this.year = year;
		this.month = month;
		this.siteid = siteid;
		this.forumid = forumid;
		this.m_repositoryEngine = repository_engine;
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.PartitionSearcher#queryItem(long)
	 */
	public TEItem queryItem(long ItemID) 
	throws RepositoryEngineException{
		int idfidx = 0;
		IDFEngine idfengine = null;
		idfengine = m_repositoryEngine.getIDFEngine(year, month, siteid, forumid, idfidx);
		long count = ItemID;
		while(count > idfengine.getMaxItemCount()){
			count = count - idfengine.getMaxItemCount();
			idfidx++;
			idfengine = m_repositoryEngine.getIDFEngine(year, month, siteid, forumid, idfidx);
		}
		try {
			return idfengine.getItem((int)count);
		} catch (IDFEngineException e) {
			throw new RepositoryEngineException(e);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.cic.textengine.repository.datanode.repository.PartitionSearcher#queryItems(java.util.ArrayList, boolean)
	 */
	public ArrayList<TEItem> queryItems(ArrayList<Long> item_id_list, boolean sorted) 
	throws RepositoryEngineException{
		ArrayList<Long> id_list = null;
		if (!sorted){
			id_list = new ArrayList<Long>(item_id_list);
			Collections.sort(id_list);
		}else{
			id_list = item_id_list;
		}
		

		ArrayList<TEItem> result = new ArrayList<TEItem>();
		if (id_list.size()<=0){//return empty result 
			return result;
		}
		
		int idfidx = 0;
		IDFEngine idfengine = null;
		idfengine = m_repositoryEngine.getIDFEngine(year, month, siteid, forumid, idfidx);
		long max_item_id = idfengine.getMaxItemCount(); 
		long current_item_index_offset = 0;
		ArrayList<Integer> item_index_list = new ArrayList<Integer>();
		long itemid = 0;	//get the first itemid, find the first idfidx
		
		for (int i = 0;i<id_list.size(); i++){
			itemid = id_list.get(i);

			if (itemid > max_item_id){
				//query items from current idf
				if (item_index_list.size() > 0){
					try {
						result.addAll(idfengine.getItems(item_index_list, true));
						item_index_list.clear();
					} catch (IDFEngineException e) {
						throw new RepositoryEngineException(e);
					}
				}
				
				while(itemid > max_item_id){
					idfidx ++;
					idfengine = m_repositoryEngine.getIDFEngine(year, month, siteid, forumid, idfidx);
					current_item_index_offset = max_item_id;
					max_item_id += idfengine.getMaxItemCount();
				}

				i--; //backword one step
			}else{
				item_index_list.add((int)(itemid - current_item_index_offset));
			}
		}

		//flush the rest buffer
		if (item_index_list.size() > 0){
			try {
				result.addAll(idfengine.getItems(item_index_list, true));
				item_index_list.clear();
			} catch (IDFEngineException e) {
				throw new RepositoryEngineException(e);
			}
		}

		return result;
	}
}
