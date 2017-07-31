package com.cic.textengine.repository.datanode.repository.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.IDFEnginePool;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;
import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.PartitionSearcher;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;

/**
 * The repository engine is reponsible for organizing a group of IDF to store 
 * text messages on the local operating system.
 * 
 * @author denis.yu
 *
 */
public class DefaultRepositoryEngineImpl implements RepositoryEngine {
	
	Logger m_logger = Logger.getLogger(DefaultRepositoryEngineImpl.class);
	
	File m_repositoryPath = null;
	
	public DefaultRepositoryEngineImpl()
	{}
	
	public void init(File repPath)
	throws RepositoryEngineException{
		if (!(repPath.exists() && repPath.isDirectory())){
			throw new RepositoryEngineException("Repository path does NOT exist.");
		}
		m_repositoryPath = repPath;
	}
	
	String buildIDFFileName(int year, int month, String siteid, String forumid, int idx) throws UnsupportedEncodingException {
	/*String b64_siteid = new String(Base64.encodeBase64(siteid.trim().getBytes(),false));
		String b64_forumid = new String(Base64.encodeBase64(forumid.trim().getBytes(),false));*/
		String hex_siteid = new String(Hex.encodeHex(siteid.trim().getBytes("utf-8")));
		String hex_forumid = new String(Hex.encodeHex(forumid.trim().getBytes("utf-8")));
//		String hex_siteid = new String(Hex.encodeHex(siteid.trim().getBytes()));
//		String hex_forumid = new String(Hex.encodeHex(forumid.trim().getBytes()));
		String idf_name = formatMonthKey(year, month) + "_" + hex_siteid 
			+ "_" + hex_forumid + "_" + idx + ".idf";
		return idf_name;
	}
	
	public IDFEngine getIDFEngine(int year, int month, String siteid, String forumid, int idx)
	throws RepositoryEngineException{
		File file = null;
		IDFEngine engine = null;

		try {
			file = new File(m_repositoryPath.getAbsoluteFile()
					+ File.separator
					+ buildIDFFileName(year, month, siteid, forumid, idx));
			
			engine = IDFEnginePool.getInstance().getIDFEngineInstance(file);
			
		} catch (IDFEngineInitException e) {
			throw new RepositoryEngineException(e);
		} catch (IDFEngineException e) {
			throw new RepositoryEngineException(e);
		} catch (UnsupportedEncodingException e) {
			throw new RepositoryEngineException(e);
		}
		
		return engine;
	}
	
	String formatMonthKey(int year, int month){
		String result = Integer.toString(year);
		if (month >= 10){
			result += Integer.toString(month);
		}else{
			result += "0" + Integer.toString(month);
		}
		return result;
	}

	public boolean clean() {
		m_logger.debug("Clean IDF Repository...");
		File[] files = m_repositoryPath.listFiles(new FilenameFilter(){
			public boolean accept(File dir,String name){
				if (name.toLowerCase().endsWith(".idf") || name.toLowerCase().endsWith(".idf.idx")){
					return true;
				}else{
					return false;
				}
			}
		});
		boolean result = true;
		for (int i = 0;i<files.length ; i++){
			m_logger.debug("Remove IDF [" + files[i].getAbsolutePath()
					+ File.separator + files[i].getName());
			result = result & files[i].delete();
		}
		
		return result;
	}

	public PartitionWriter getPartitionWriter(int year, int month,
			String siteid, String forumid, long startItemID)
			throws RepositoryEngineException {
		DefaultPartitionWriterImpl writer = new DefaultPartitionWriterImpl(this,year,
				month, siteid, forumid, startItemID);
		return writer;
	}

	public PartitionSearcher getPartitionSearcher(int year, int month,
			String siteid, String forumid) throws RepositoryEngineException {
		DefaultPartitionSearcherImpl searcher = new DefaultPartitionSearcherImpl(this,year,
				month, siteid, forumid);
		return searcher;
	}

	public PartitionEnumerator getPartitionEnumerator(int year, int month,
			String siteid, String forumid) throws RepositoryEngineException {
		return getPartitionEnumerator(year, month, siteid, forumid,1, false);
	}

	public PartitionEnumerator getPartitionEnumerator(int year, int month,
			String siteid, String forumid, long startItemID, boolean includeDeletedItems) throws RepositoryEngineException {
		DefaultPartitionEnumeratorImpl enu = new DefaultPartitionEnumeratorImpl(
				this, year, month, siteid, forumid, startItemID,  includeDeletedItems);
		return enu;
	}

	public void deleteItems(int year, int month, String siteid, String forumid,
			ArrayList<Long> itemids, boolean sorted) throws RepositoryEngineException {
		ArrayList<Long> id_list = null;
		if (!sorted){
			id_list = new ArrayList<Long>(itemids);
			Collections.sort(id_list);
		}else{
			id_list = itemids;
		}
		

		if (id_list.size()<=0){//return empty result 
			return;
		}
		
		int idfidx = 0;
		IDFEngine idfengine = null;
		idfengine = this.getIDFEngine(year, month, siteid, forumid, idfidx);
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
						idfengine.deleteItems(item_index_list, true);
						item_index_list.clear();
					} catch (IDFEngineException e) {
						throw new RepositoryEngineException(e);
					}
				}
				
				while(itemid > max_item_id){
					idfidx ++;
					idfengine = this.getIDFEngine(year, month, siteid, forumid, idfidx);
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
				idfengine.deleteItems(item_index_list, true);
				item_index_list.clear();
			} catch (IDFEngineException e) {
				throw new RepositoryEngineException(e);
			}
		}

		return;
	}
}
