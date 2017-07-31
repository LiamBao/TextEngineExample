package com.cic.textengine.repository.partitionlock;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The class manages write lock for all partition on the name node. 
 * 
 * @author denis.yu
 *
 */
public class PartitionWriteLockManager {
	static PartitionWriteLockManager m_instance = null;
	
	Hashtable<Integer, Hashtable<Integer, Hashtable<String, Hashtable<String, PartitionWriteLock>>>> m_LockTree = null;
	ArrayList<PartitionWriteLock> m_lockList = new ArrayList<PartitionWriteLock>();
	
	
	public static synchronized PartitionWriteLockManager getInstance(){
		if (m_instance == null){
			m_instance = new PartitionWriteLockManager();
		}
		return m_instance;
	}
	
	PartitionWriteLockManager(){
		m_LockTree = new Hashtable<Integer, Hashtable<Integer, Hashtable<String, Hashtable<String, PartitionWriteLock>>>>();
	}
	
	public ArrayList<PartitionWriteLock> listExistingLock(){
		ArrayList<PartitionWriteLock> list = new ArrayList<PartitionWriteLock>();
		list.addAll(m_lockList);
		return list;
	}
	
	
	/**
	 * Apply a write lock for a particular partition. If the partition is already locked,
	 * the exception will be thrown. 
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws PartitionWriteLockManagerException
	 */
	public synchronized PartitionWriteLock applyLock(int year, 
			int month, 
			String siteid, 
			String forumid, 
			String ip,
			String data_node_key, 
			int operation)
	throws PartitionAlreadyLockedException{
		
		//validate data node
		
		
		Hashtable<Integer, Hashtable<String, Hashtable<String, PartitionWriteLock>>> hash_month = m_LockTree.get(year);
		if (hash_month == null){
			hash_month = new Hashtable<Integer, Hashtable<String, Hashtable<String, PartitionWriteLock>>>();
			m_LockTree.put(year, hash_month);
		}
		
		Hashtable<String, Hashtable<String, PartitionWriteLock>> hash_site = hash_month.get(month);
		if (hash_site == null){
			hash_site = new Hashtable<String, Hashtable<String, PartitionWriteLock>>();
			hash_month.put(month, hash_site);
		}
		
		Hashtable<String, PartitionWriteLock> hash_forum = hash_site.get(siteid);
		if (hash_forum == null){
			hash_forum = new Hashtable<String, PartitionWriteLock>();
			hash_site.put(siteid, hash_forum);
		}
		
		PartitionWriteLock lock = hash_forum.get(forumid);
		if (lock == null){
			lock = new PartitionWriteLock(year, month, siteid, forumid, ip,
					data_node_key, operation);
			hash_forum.put(forumid, lock);
		}else{
			throw new PartitionAlreadyLockedException(year, month, siteid, forumid);
		}
		
		m_lockList.add(lock);
		return lock;
	}

	/**
	 * Get the lock for partition. If the partition is not locked, null will be returned.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 */
	public synchronized PartitionWriteLock getLock(int year, int month, String siteid, String forumid)
	{
		Hashtable<Integer, Hashtable<String, Hashtable<String, PartitionWriteLock>>> hash_month = m_LockTree.get(year);
		if (hash_month == null){
			return null;
		}
		
		Hashtable<String, Hashtable<String, PartitionWriteLock>> hash_site = hash_month.get(month);
		if (hash_site == null){
			return null;
		}
		
		Hashtable<String, PartitionWriteLock> hash_forum = hash_site.get(siteid);
		if (hash_forum == null){
			return null;
		}
		
		PartitionWriteLock lock = hash_forum.get(forumid);
		return lock;
	}
	
	/**
	 * Release a paritition lock.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param ip
	 * @param name_node_key
	 * @throws PartitionWriteLockManagerException
	 */
	public synchronized void releaseLock(int year, 
			int month, 
			String siteid, 
			String forumid,
			String ip,
			String data_node_key, 
			int operation) 
	throws NoPartitionLockFoundException, IllegalPartitionLockStatusException
	{
		PartitionWriteLock lock = null;
		Hashtable<Integer, Hashtable<String, Hashtable<String, PartitionWriteLock>>> hash_month = m_LockTree.get(year);
		if (hash_month == null){
			throw new NoPartitionLockFoundException("No year hash found.");
		}
		
		Hashtable<String, Hashtable<String, PartitionWriteLock>> hash_site = hash_month.get(month);
		if (hash_site == null){
			throw new NoPartitionLockFoundException("No month hash found.");
		}
		
		Hashtable<String, PartitionWriteLock> hash_forum = hash_site.get(siteid);
		if (hash_forum == null){
			throw new NoPartitionLockFoundException("No siteid hash found.");
		}
		
		lock = hash_forum.get(forumid);

		if (lock == null){
			throw new NoPartitionLockFoundException("No forumid hash found.");
		}else{
			if (lock.getIP().equals(ip) && lock.getDataNodeKey().equals(data_node_key)){
				if (operation == 0 || lock.getOperation() == operation){
					hash_forum.remove(forumid);
					m_lockList.remove(lock);
				}else{
					throw new IllegalPartitionLockStatusException();
				}
			}else{
				throw new IllegalPartitionLockStatusException();
			}
		}
	}
}
