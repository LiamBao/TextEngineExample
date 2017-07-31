package com.cic.textengine.idf;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;

import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;

/**
 * @author denis.yu
 *
 */
public class IDFEnginePool {
	static IDFEnginePool m_instance = null;
	
	int poolSize = 1000;
	
	
	Hashtable <File, IDFEngine> m_cache = new Hashtable<File, IDFEngine>();
	ArrayList <File> m_index = new ArrayList<File>();
	
	long m_count_hit = 0;
	long m_count_miss = 0;
	long m_count_swith = 0;
	
	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public static synchronized IDFEnginePool getInstance(){
		if (m_instance == null){
			m_instance = new IDFEnginePool();
		}
		return m_instance;
	}
	
	void resetCounter(){
		m_count_hit = 0;
		m_count_miss = 0;
		m_count_swith = 0;
	}
	
	public synchronized IDFEngine getIDFEngineInstance(File file) 
	throws IDFEngineInitException, IDFEngineException{
		return IDFEngineFactory.getNewIDFEngineInstance(file);
		
//		IDFEngine engine = m_cache.get(file);
//		if (engine == null){
//			try{
//				m_count_miss++;
//			}catch(Exception e){
//				resetCounter();
//			}
//			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
//			//push the new obj into the cache
//			if (m_index.size() >= this.getPoolSize()){
//				try{
//					m_count_swith++;
//				}catch(Exception e){
//					resetCounter();
//				}
//				//need to remove one from the cache;
//				File file_to_be_removed = m_index.get(0);
//
//				m_cache.remove(file_to_be_removed);
//				m_index.remove(0);
//			}
//			m_cache.put(file, engine);
//			m_index.add(file);
//		}else{
//			//hit the cache, move the file to the bottom of the list
//			try {
//				m_count_hit++;
//			} catch (Exception e) {
//				resetCounter();
//			}
//			m_index.remove(file);
//			m_index.add(file);
//		}
//		return engine;
	}
	
	public float getHitRatio(){
		if (m_count_hit + m_count_miss == 0){
			return 0;
		}else{
			return m_count_hit / (m_count_hit + m_count_miss);
		}
	}
}
