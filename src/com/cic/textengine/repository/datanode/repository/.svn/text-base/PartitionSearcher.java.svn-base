package com.cic.textengine.repository.datanode.repository;

import java.util.ArrayList;

import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

public interface PartitionSearcher {

	public abstract TEItem queryItem(long ItemID)
			throws RepositoryEngineException;

	public abstract ArrayList<TEItem> queryItems(ArrayList<Long> item_id_list,
			boolean sorted) throws RepositoryEngineException;

}