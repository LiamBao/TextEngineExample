package com.cic.textengine.repository.namenode.dnregistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.Logger;

/**
 * This class is a runtime Registry table for regisitng all active Datanode and Partition.
 * 
 * The registry keep a list 
 * 
 * @author denis.yu
 *
 */
public class DNRegistryTable {
	Logger m_logger = Logger.getLogger(DNRegistryTable.class);
	
	static DNRegistryTable m_instance = null;

	Hashtable<String, DNRegistry> m_DNHash = new Hashtable<String, DNRegistry>();
	
	public static synchronized DNRegistryTable getInstance(){
		if (m_instance == null){
			m_instance = new DNRegistryTable();
		}
		return m_instance;
	}
	
	DNRegistryTable(){
		(new DNExpiringThread()).start();
	}

	public void registerDN(DNRegistry registry){
		DNRegistry reg = m_DNHash.get(registry.getDNKey());
		if (reg == null){
			m_logger.debug("New activate data node found [DNKey:" + registry.getDNKey() + "]");
			m_DNHash.put(registry.getDNKey(), registry);
		}else{
			reg.resetTTL();
			reg.setFreeSpace(registry.getFreeSpace());
		}
	}
	
	public void unregisterDN(String dnkey){
		m_logger.debug("De-activate data node [DNKey:" + dnkey + "]");
		m_DNHash.remove(dnkey);
	}
	
	public DNRegistry getDNRegistry(String dnkey){
		return m_DNHash.get(dnkey);
	}
	
	public ArrayList<DNRegistry> listDNRegistries(){
		ArrayList<DNRegistry> list = new ArrayList<DNRegistry>();
		list.addAll(m_DNHash.values());
		return list;
	}

	public ArrayList<DNRegistry> listDNRegistriesOrderByPartitionCountAsc(){
		ArrayList<DNRegistry> list = new ArrayList<DNRegistry>();
		list.addAll(m_DNHash.values());

		Collections.sort(list, new Comparator<DNRegistry>(){
			public int compare(DNRegistry arg0, DNRegistry arg1) {
				if (arg0.getPartitionCount() < arg1.getPartitionCount()){
					return -1;
				}else if (arg0.getPartitionCount() == arg1.getPartitionCount()){
					return 0;
				}else{
					return 1;
				}
			}
		});
		return list;
	}
	
	public ArrayList<DNRegistry> listDNRegistriesOrderByFreeSpaceDesc() {
		ArrayList<DNRegistry> list = new ArrayList<DNRegistry>();
		list.addAll(m_DNHash.values());

		// make the DN with smaller free space "larger" because the default sort
		// result is in ascending order.
		Collections.sort(list, new Comparator<DNRegistry>() {
			public int compare(DNRegistry arg0, DNRegistry arg1) {
				if (arg0.getFreeSpace() < arg1.getFreeSpace()) {
					return 1;
				} else if (arg0.getFreeSpace() == arg1.getFreeSpace()) {
					return 0;
				} else {
					return -1;
				}
			}
		});
		for(DNRegistry dn: list){
			m_logger.debug(dn.host+"\t"+dn.freeSpace);			
		}
		return list;
	}
	
	class DNExpiringThread extends Thread{
		Logger m_logger = Logger.getLogger(DNExpiringThread.class);
		DNExpiringThread(){
			this.setDaemon(true);
		}
		
		public void run(){
			Iterator<DNRegistry> list = m_DNHash.values().iterator();
			DNRegistry dnregistry = null;
			long ts_previous;
			ts_previous = System.currentTimeMillis();
			while(true){
				while(list.hasNext()){
					dnregistry = list.next();
					dnregistry.setTTL(dnregistry.getTTL() - (System.currentTimeMillis() - ts_previous));
					if (dnregistry.getTTL()<=0){
						m_logger.debug("DN not active for too long time, expire the DN.");
						unregisterDN(dnregistry.getDNKey());
					}
				}

				ts_previous = System.currentTimeMillis();
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					//ignore 
				}
			}
		}
	}
}
