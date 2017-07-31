package com.cic.textengine.repository.datanode.repository;

import java.io.File;

import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.datanode.repository.impl.DefaultRepositoryEngineImpl;



public class RepositoryFactory {
	public static synchronized RepositoryEngine getNewRepositoryEngineInstance(String repositoryFolder) 
	throws RepositoryEngineException{
		
		RepositoryEngine engine = null;
		engine = new DefaultRepositoryEngineImpl();
		File file = new File(repositoryFolder);
		engine.init(file);
		
		return engine;
	}
}
