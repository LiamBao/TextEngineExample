package com.cic.textengine.repository.namenode.manager;

import java.util.ArrayList;

import com.cic.textengine.repository.namenode.manager.exception.DataNodeIPAlreadyExistsException;
import com.cic.textengine.repository.namenode.manager.exception.IllegalNameNodeException;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.exception.NoDataNodeAvaliableForPartitionWrite;
import com.cic.textengine.repository.namenode.manager.type.DNPartitionUpgradeVersion;
import com.cic.textengine.repository.namenode.manager.type.OLogItem;

/**
 * The manager of namenode local repository
 * @author denis.yu
 *
 */
public interface NameNodeManager {
	
	/**
	 * Activate a data node by the dn key and dn ip. 
	 * 
	 * @param dndaemon_ip
	 * @param key
	 * @throws NameNodeManagerException
	 * @throws IllegalNameNodeException		If the key and ip are not matched, this excpeiton will be thrown
	 */
	public void activateNameNode(String dndaemon_ip, String key)
	throws NameNodeManagerException,IllegalNameNodeException;

	/**
	 * Assign a partition to a data node
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param dnkey
	 * @throws NameNodeManagerException
	 */
	public void assignDNPartition(int year, int month, String siteid,
			String forumid, String dnkey) throws NameNodeManagerException;
	
	/**
	 * List all partition version for a data node.
	 * 
	 * @param dnkey
	 * @return
	 * @throws NameNodeManagerException
	 */
	public ArrayList<DNPartitionUpgradeVersion> listDNPartitionUpgradeVersion(
			String dnkey) throws NameNodeManagerException;

	/**
	 * List the number of partitions registered on this data node.
	 * 
	 * @param dnkey
	 * @return
	 * @throws NameNodeManagerException
	 */
	public int getDNPartitionCount(String dnkey) throws NameNodeManagerException;
	
	/**
	 * Clean a partition. The data in the partition will not be deleted. But the partition
	 * on the name node will be marked to be empty.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return	The new partition version
	 * @throws NameNodeManagerException
	 */
	public int cleanPartition(int year, int month, String siteid, String forumid) throws NameNodeManagerException;
	
	/**
	 * Finish the delete operation on the data node. 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param targetVersion
	 * @param dnkeys
	 * @return	The new partition version.
	 * @throws NameNodeManagerException
	 */
	public int finishPartitionDelete(int year, int month, String siteid,
			String forumid,int targetVersion,ArrayList<String> dnkeys
			) throws NameNodeManagerException ;
	
	/**
	 * Finish the partition add operation on data node.
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param startItemID
	 * @param itemCount
	 * @param DNKeyList
	 * @return	The new partition version.
	 * @throws NameNodeManagerException
	 */
	public int finishPartitionWrite(int year, int month, String siteid,
			String forumid, long startItemID, long itemCount,
			ArrayList<String> DNKeyList)
	throws NameNodeManagerException;
	
	/**
	 * Retrieve a data node for write items into the partition.
	 * 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeManagerException
	 */
	public ArrayList<String> getDataNodeForPartitionWrite(int year, int month,
			String siteid, String forumid) throws NameNodeManagerException,
			NoDataNodeAvaliableForPartitionWrite;
	
	/**
	 * Check if a data node is available for appending new items.
	 * 
	 * Some time one data node will be contains the most updated items. In this case
	 * this data node can not be used to append new data.
	 * 
	 * @param dnkey
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return Return -1 if this data node doesnot contains the latest 
	 * 			 partition data or it is not registered for this partition.  Otherwise
	 * 			return the partition append point (startItemID).
	 * @throws NameNodeManagerException
	 */
	public long getDNPartitionAppendPoint(String dnkey, int year, 
			int month, String siteid, String forumid)
	throws NameNodeManagerException;

	/**
	 * Get the item count for a partition on one data node.
	 * s
	 * @param dnkey
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeManagerException
	 */
	public long getDNPartitionItemCount(String dnkey, int year, int month,
			String siteid, String forumid) throws NameNodeManagerException;

	public OLogItem getNextDNPartitionOperation(String dnkey,
			int year, int month, String siteid, String forumid)
			throws NameNodeManagerException;

	/**
	 * Check if a data node has the latest version of partition. 
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param dnkey
	 * @return
	 * @throws NameNodeManagerException
	 */
	public boolean isDNPartitionVersionLatest(int year, int month,
			String siteid, String forumid, String dnkey)
			throws NameNodeManagerException ;


	/**
	 * Retrieve all DNkeys for a partition.
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeManagerException
	 */
	public ArrayList<String> listPartitionDN(int year, int month, String siteid, String forumid) throws NameNodeManagerException;

	/**
	 * List data node for query partition data
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 * @throws NameNodeManagerException
	 */
	public ArrayList<String> listPartitionDNForQuery(int year, int month,
			String siteid, String forumid) throws NameNodeManagerException;

		
	public int logPartitionDeleteOperation(int year, int month, String siteid,
			String forumid, ArrayList<Long> itemIDList, boolean IDSorted)
	throws NameNodeManagerException ;

	/**
	 * Register a new data node
	 * @param dndaemon_ip
	 * @param dndaemon_port
	 * 
	 * @return The global unique key for the data node.
	 * @throws NameNodeManagerException 
	 * @throws DataNodeIPAlreadyExistsException 
	 */
	public String regsiterDataNode(String dndaemon_ip, int dndaemon_port) throws NameNodeManagerException, DataNodeIPAlreadyExistsException;

	
	
	/**
	 * Upgrade the partition version for a partition to one version above.
	 * @param dnkey
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param version
	 * @throws NameNodeManagerException
	 */
	public void updateDNPartitionVersion(String dnkey, int year, int month, String siteid,
			String forumid,long itemCount, int version) throws NameNodeManagerException;

	public boolean validateDataNode(String key, String ip) throws NameNodeManagerException;
	
	public void cleanNameNodeCache() throws NameNodeManagerException;
}
