package com.cic.textengine.repository.importer;

import java.io.IOException;

import org.apache.commons.codec.DecoderException;
import org.apache.log4j.Logger;

import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import com.cic.textengine.posttrend.PostTrend;
import com.cic.textengine.repository.ItemImporter;
import com.cic.textengine.repository.datanode.type.PartitionRecord;
import com.cic.textengine.repository.importer.exception.ImporterProcessException;
import com.cic.textengine.repository.importer.exception.ProcessUpdatePosttrendException;
import com.cic.textengine.repository.type.PartitionKey;

public class ProcessUpdatePosttrend implements ImporterProcess {

	private static Logger m_logger = Logger.getLogger(ProcessUpdatePosttrend.class);
	private JDBMManager jdbmMan = null;
	
	public void process(ItemImporterPerformanceLogger perfLogger)
			throws ImporterProcessException {

		// JDBM Set Up
		BTree parKeyStartItemIDBTree = null;
		TupleBrowser btree_browser = null;
		Tuple record = new Tuple();
		try {
			jdbmMan = JDBMManager.getInstance(ItemImporter.DATABASE);
			parKeyStartItemIDBTree = jdbmMan
					.getBTree(ItemImporter.BTREE_PARTITION_KEY_STARTITEMID);
			btree_browser = parKeyStartItemIDBTree.browse();
		} catch (IOException e) {
			throw new ProcessUpdatePosttrendException(e);
		}
		
		// check if there is partition info to be updated.
		if (parKeyStartItemIDBTree.size() <=0){
			// close JDBM
			try {
				jdbmMan.close();
			} catch (IOException e) {
				m_logger.error("Error closing JDBM");
			}
			m_logger.info("No Post Trend Info to Be Updated.");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e3) {
				// ignore
			}
			return;
		}
		
		PostTrend postTrend = new PostTrend();
		
		//browse the JDBM, update the post trend and remove the record
		try {
			while(btree_browser.getNext(record)) {
				String keyStr = record.getKey().toString();
				PartitionRecord start_count = (PartitionRecord) (record
						.getValue());
				int itemCount = start_count.getItemCount();
				PartitionKey parkey = PartitionKey.decodeStringKey(keyStr);
				postTrend.addTrend(parkey, itemCount);
				jdbmMan.remove(parKeyStartItemIDBTree, keyStr);
				jdbmMan.commit();
			}
		} catch (IOException e) {
			// JDBM throw exception here
			m_logger.error("Error in browsing items from JDBM.");
			throw new ProcessUpdatePosttrendException(e); 
		} catch (DecoderException e) {
			// partition key decode exception
			m_logger.error("Error in decode the partition key.");
			throw new ProcessUpdatePosttrendException(e); 
		} catch (Exception e) {
			// post trend update exception error here
			m_logger.error("Error in update post trend.");
			throw new ProcessUpdatePosttrendException(e); 
		}
		
		// check if there is still some partition info left.
		if (parKeyStartItemIDBTree.size() > 0){
			// close JDBM
			try {
				jdbmMan.close();
				postTrend.close();
			} catch (IOException e) {
				m_logger.error("Error closing JDBM");
			}
			m_logger.info("More Post Trend Info to Be Updated.");
			throw new ProcessUpdatePosttrendException("More Post Trend Info to Be Updated.");
		}
		
		// close JDBM finally
		try {
			jdbmMan.close();
			postTrend.close();
		} catch (IOException e) {
			m_logger.error("Error closing JDBM");
		}
	}

}
