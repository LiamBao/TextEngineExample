package com.cic.textengine.repository.namenode.strategy;

import java.util.ArrayList;

import com.cic.textengine.repository.namenode.manager.type.DataNode;

/**
 * Implement the algorithm how too choose the data node for different purpose.
 * Please note that all dn keys pass to this strategy should be the data nodes
 * which are qualified for the corresponding operation. This strategy won't verify
 * if if input dnkey is qualified for the operation. 
 * 
 * For example, for partition add operation, all input dnkeys should point to the 
 * data node which has the latest version of that partition.
 * 
 * @author denis.yu
 *
 */
public interface DNChooseStrategy {
	/**
	 * Chose a data for for sync the partition add operation.
	 * @param dnkeys
	 * @return
	 */
	public DataNode chooseDNForPartitionAddSync(ArrayList<String> dnkeys);
	
	/**
	 * Chose a data node for query from a list of qualified datanode key list.
	 * @param dnkey_list
	 * @return
	 */
	public DataNode chooseDNForQuery(ArrayList<String> dnkey_list);

		/**
	 * Return all dn list that should replicate the partition over.
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @return
	 */
	public ArrayList<DataNode> chooseDNForPartitionReplication(int year,
			int month, String siteid, String forumid);

	/**
	 * Get a data node for partition write operation
	 * @param year
	 * @param month
	 * @param siteid
	 * @param forumid
	 * @param dnkeys
	 * @return
	 */
	public DataNode chooseDNForPartitionWrite(int year,
			int month, String siteid, String forumid, ArrayList<String> dnkeys);
}
