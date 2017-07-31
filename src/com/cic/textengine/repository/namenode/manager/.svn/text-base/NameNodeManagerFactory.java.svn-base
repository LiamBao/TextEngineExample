package com.cic.textengine.repository.namenode.manager;

import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.impl.DefaultNameNodeManagerImpl;

public class NameNodeManagerFactory {
	static NameNodeManager m_instance = null; 
	
	public static synchronized NameNodeManager getNameNodeManagerInstance() 
	throws NameNodeManagerException{
		if (m_instance == null){
			m_instance = new DefaultNameNodeManagerImpl();
		}
		return m_instance;
	}
}
