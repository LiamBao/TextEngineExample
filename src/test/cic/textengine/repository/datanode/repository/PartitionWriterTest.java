package test.cic.textengine.repository.datanode.repository;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class PartitionWriterTest {

	@Test
	public void testWriteItems() {
		RepositoryEngine engine;
		try {
			engine = RepositoryFactory.getNewRepositoryEngineInstance("d:\\");
			PartitionWriter pw = null;
			long startItemID = 1;
			for (int idx = 0;idx < 100;idx++){
				pw = engine.getPartitionWriter(2007, 12, "1", "2", startItemID);
				
				for (int i = 0;i<10000;i++){
					pw.writeItem(getSampleItem());
				}
				pw.flush();
				pw.close();
				
				startItemID += 10000;
			}
		} catch (RepositoryEngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
	}

	TEItem getSampleItem(){
		TEItem item = new TEItem();
		item.setContent("Hello,world");
		item.setSubject("Hello, world");
		TEItemMeta meta = new TEItemMeta();
		meta.setDateOfPost(System.currentTimeMillis());
		meta.setFirstExtractionDate(System.currentTimeMillis());
		meta.setForumID("abc");
		meta.setForumName("abc");
		meta.setForumUrl("http://www.google.com");
		meta.setItem(item);
		meta.setItemID(123);
		meta.setItemType("BBS");
		meta.setItemUrl("http://www.seeisee.com");
		meta.setKeyword("keyword");
		meta.setKeywordGroup("kg");
		meta.setLatestExtractionDate(System.currentTimeMillis());
		meta.setPoster("poster");
		meta.setPosterID("poster");
		meta.setSimpleDateOfPost("2007-1-1");
		meta.setSiteID(1);
		meta.setSiteName("cic");
		meta.setSource("cic");
		meta.setSubject("dd");
		meta.setThreadID(123);
		meta.setTopicPost(true);
		
		item.setMeta(meta);
		return item;
	}
}
