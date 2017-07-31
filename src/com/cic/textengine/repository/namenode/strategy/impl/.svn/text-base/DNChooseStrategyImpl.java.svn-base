package com.cic.textengine.repository.namenode.strategy.impl;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.repository.namenode.dnregistry.DNRegistry;
import com.cic.textengine.repository.namenode.dnregistry.DNRegistryTable;
import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;
import com.cic.textengine.repository.namenode.manager.type.DataNode;
import com.cic.textengine.repository.namenode.strategy.DNChooseStrategy;

public class DNChooseStrategyImpl implements DNChooseStrategy {
	Logger m_logger = Logger.getLogger(DNChooseStrategyImpl.class);
	
	int replica = 2;
	
	public DataNode chooseDNForQuery(ArrayList<String> dnkey_list){
		DNRegistry dnregistry = null;
		DNRegistry final_dnregistry = null;
		
		for (String dnkey:dnkey_list){
			dnregistry = DNRegistryTable.getInstance().getDNRegistry(dnkey);
			if (dnregistry != null){
				if (final_dnregistry == null){
					final_dnregistry = dnregistry;
				}else{
					if (DN1BetterThanDN2ForReading(dnregistry, final_dnregistry)){
						final_dnregistry = dnregistry;
					}
				}
			}
		}

		DataNode res = null;
		if (final_dnregistry != null){
			res = new DataNode();
			res.setIP(final_dnregistry.getHost());
			res.setKey(final_dnregistry.getDNKey());
			res.setPort(final_dnregistry.getPort());
			final_dnregistry.increaseReadingCount();
		}
		return res;
	}
	
	public DataNode chooseDNForPartitionAddSync(ArrayList<String> dnkeys) {
		//very dummy implementation now, just return the first active data node it can find.
		
		if (dnkeys == null)
			return null;
		if (dnkeys.size()<=0)
			return null;
		
		DNRegistry dnreg = null;
		for (String dnkey: dnkeys){
			dnreg = DNRegistryTable.getInstance().getDNRegistry(dnkey);
			if (dnreg != null)
				break;
		}

		DataNode res = null;
		if (dnreg != null){
			res = new DataNode();
			res.setIP(dnreg.getHost());
			res.setKey(dnreg.getDNKey());
			res.setPort(dnreg.getPort());
		}
		return res;
	}


	/**
	 * Compare which dn is more suitable for reading
	 * @param dn1
	 * @param dn2
	 * @return
	 */
	boolean DN1BetterThanDN2ForReading(DNRegistry dn1, DNRegistry dn2){
		if (dn1.getWritingCount() == dn2.getWritingCount()){
			if (dn1.getReadingCount() < dn2.getReadingCount()){
				return true;
			}else{
				return false;
			}
		}else{
			return dn1.getWritingCount() < dn2.getWritingCount();
		}
	}
	

	public DataNode chooseDNForPartitionWrite(int year,
			int month, String siteid, String forumid, ArrayList<String> dnkeys){
		DataNode result = null;
		if (dnkeys == null || dnkeys.size()<=0){
//			ArrayList<DNRegistry> active_dn_list = DNRegistryTable.getInstance()
//			.listDNRegistriesOrderByPartitionCountAsc();
			ArrayList<DNRegistry> active_dn_list = DNRegistryTable.getInstance()
			.listDNRegistriesOrderByFreeSpaceDesc();
			for (int i = 0;i<replica && i<active_dn_list.size();i++){
				try {
					NameNodeManagerFactory.getNameNodeManagerInstance()
							.assignDNPartition(year, month, siteid, forumid,
									active_dn_list.get(i).getDNKey());
					result = new DataNode();
					result.setIP(active_dn_list.get(i).getHost());
					result.setKey(active_dn_list.get(i).getDNKey());
					result.setPort(active_dn_list.get(i).getPort());
					active_dn_list.get(i).increasePartitionCount();
					active_dn_list.get(i).increaseWritingCount();
					
				} catch (NameNodeManagerException e) {
					m_logger.error("Fail to assign DN to partition in database.",e);
				}
			}
			return result;
		}else{
			DNRegistry dnreg = null;
			for (String dnkey: dnkeys){
				dnreg = DNRegistryTable.getInstance().getDNRegistry(dnkey);
				if (dnreg != null)
					break;
			}

			if (dnreg != null){
				dnreg.increaseWritingCount();
				result = new DataNode();
				result.setIP(dnreg.getHost());
				result.setKey(dnreg.getDNKey());
				result.setPort(dnreg.getPort());
			}
			return result;
		}
		
	}
	
	public ArrayList<DataNode> chooseDNForPartitionReplication(int year,
			int month, String siteid, String forumid) {
		ArrayList<DataNode> list = new ArrayList<DataNode>();
		ArrayList<String> dnkeys;
		try {
			dnkeys = NameNodeManagerFactory
					.getNameNodeManagerInstance().listPartitionDN(year, month,
							siteid, forumid);
		} catch (NameNodeManagerException e) {
			m_logger.error("Exception", e);
			return list;
		}
		DNRegistry dnreg = null;
		int count = 0;
		for (String dnkey: dnkeys){
			dnreg = DNRegistryTable.getInstance().getDNRegistry(dnkey);
			if (dnreg != null){
				DataNode dn = new DataNode();
				dn.setIP(dnreg.getHost());
				dn.setKey(dnreg.getDNKey());
				dn.setPort(dnreg.getPort());
				list.add(dn);
				count++;
			}
		}
		if (count >= replica)
			return list;
		
//		ArrayList<DNRegistry> active_dn_list = DNRegistryTable.getInstance()
//		.listDNRegistriesOrderByPartitionCountAsc();
		ArrayList<DNRegistry> active_dn_list = DNRegistryTable.getInstance()
		.listDNRegistriesOrderByFreeSpaceDesc();

		for(int i = 0;i<active_dn_list.size() && count < replica;i++){
			dnreg = active_dn_list.get(i);
			if (!dnkeys.contains(dnreg.getDNKey())){
				m_logger.debug("Assign partition to data node [key:"
						+ dnreg.getDNKey() + ",pCount:"
						+ dnreg.getPartitionCount());
				try {
					NameNodeManagerFactory.getNameNodeManagerInstance()
							.assignDNPartition(year, month, siteid, forumid,
									dnreg.getDNKey());
				} catch (NameNodeManagerException e) {
					m_logger.error("Fail to assign DN to partition in database.",e);
				}
	
				dnreg.increasePartitionCount();
				
				DataNode dn = new DataNode();
				dn.setIP(dnreg.getHost());
				dn.setKey(dnreg.getDNKey());
				dn.setPort(dnreg.getPort());
				list.add(dn);
				count++;
			}
		}
		
		m_logger.debug(list.size() + " Datanodes are chosen for partiton replication.");
		return list;
	}

	
}
