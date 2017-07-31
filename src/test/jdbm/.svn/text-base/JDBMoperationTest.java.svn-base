package test.jdbm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import com.cic.textengine.repository.ItemImporter;
import com.cic.textengine.repository.datanode.type.PartitionRecord;
import com.cic.textengine.repository.importer.JDBMManager;

public class JDBMoperationTest {
	@Test
	public void testRemove() throws IOException
	{
		JDBMManager jdbmMan = JDBMManager.getInstance(ItemImporter.DATABASE);
		
		BTree parKeyBTree = jdbmMan.getBTree(ItemImporter.BTREE_PARTITION_KEY);
		
		BTree parKeyStartItemIDBTree = jdbmMan.getBTree(ItemImporter.BTREE_PARTITION_KEY_STARTITEMID);
		
		String parkey = "4646323730_2008_46494463323634464944_7";
		Tuple tuple = new Tuple();
		
		HashMap<String, Long> parkeyMap = new HashMap<String, Long>();
		TupleBrowser btree_browser = parKeyBTree.browse();
		try {
			while(btree_browser.getNext(tuple))
			{
				parkeyMap.put((String)tuple.getKey(), (Long)tuple.getValue());
				System.out.println(tuple.getKey()+":"+tuple.getValue());
			}
		} catch (IOException e)
		{
			System.out.println(e.getLocalizedMessage());
		}

		HashMap<String, PartitionRecord> startItemIDMap = new HashMap<String, PartitionRecord>();
		btree_browser = parKeyStartItemIDBTree.browse();
		try {
			while(btree_browser.getNext(tuple))
			{
				PartitionRecord record = (PartitionRecord)tuple.getValue();
				startItemIDMap.put((String)tuple.getKey(), record);
				System.out.println(tuple.getKey()+":"+record.getStartItemID()+"_"+record.getItemCount());
			}
		} catch (IOException e)
		{
			System.out.println(e.getLocalizedMessage());
		}
		
//		for(int i=0; i<recordList.size(); i++)
//		{
//			PartitionRecord record = new PartitionRecord(1, 1);
//			try {
//				parKeyStartItemIDBTree.insert(parkey, record, true);
//			} catch (IOException e) {
//				System.out.println(e.getLocalizedMessage());
//			}
//		}

		jdbmMan.close();
		
		JDBMManager jdbmMan_new = JDBMManager.getInstance("ItemImporterDBNew");
		BTree parKeyBTree_new = jdbmMan_new.getBTree(ItemImporter.BTREE_PARTITION_KEY);
		BTree parKeyStartItemIDBTree_new = jdbmMan_new.getBTree(ItemImporter.BTREE_PARTITION_KEY_STARTITEMID);
		
		for(String key: parkeyMap.keySet())
		{
			parKeyBTree_new.insert(key, parkeyMap.get(key), true);
		}

		for(String key: startItemIDMap.keySet())
		{
			parKeyStartItemIDBTree_new.insert(key, startItemIDMap.get(key), true);
		}
		
		btree_browser = parKeyBTree_new.browse();
		try {
			while(btree_browser.getNext(tuple))
			{
				System.out.println(tuple.getKey()+":"+tuple.getValue());
			}
		} catch (IOException e)
		{
			System.out.println(e.getLocalizedMessage());
		}
		
		btree_browser = parKeyStartItemIDBTree_new.browse();
		try {
			while(btree_browser.getNext(tuple))
			{
				PartitionRecord record = (PartitionRecord)tuple.getValue();
				System.out.println(tuple.getKey()+":"+record.getStartItemID()+"_"+record.getItemCount());
			}
		} catch (IOException e)
		{
			System.out.println(e.getLocalizedMessage());
		}
		jdbmMan_new.close();
	}

}
