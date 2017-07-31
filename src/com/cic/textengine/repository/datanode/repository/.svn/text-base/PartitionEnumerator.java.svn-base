package com.cic.textengine.repository.datanode.repository;

import java.io.IOException;

import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;

public interface PartitionEnumerator {

	public abstract boolean next() throws IOException,
			RepositoryEngineException;

	public abstract TEItem getItem();

	public abstract void close() throws IOException;

}