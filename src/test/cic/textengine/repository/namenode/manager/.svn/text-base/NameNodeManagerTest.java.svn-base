package test.cic.textengine.repository.namenode.manager;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;

import com.cic.textengine.repository.namenode.manager.NameNodeManagerFactory;
import com.cic.textengine.repository.namenode.manager.exception.NameNodeManagerException;

public class NameNodeManagerTest {

	public void testFinishPartitionWrite(){
		try {
			ArrayList<String> dnkeylist = new ArrayList<String>();
			dnkeylist.add("d3c6bfe8-ee9d-46d6-ba19-a17ad90a348f");
			
			NameNodeManagerFactory.getNameNodeManagerInstance()
					.finishPartitionWrite(
							2007,12,"1","2",101,50,dnkeylist);
		} catch (NameNodeManagerException e) {
			e.printStackTrace();
			fail();
		}
		
	}

	public void testCleanPartition(){
		try {
			ArrayList<String> dnkeylist = new ArrayList<String>();
			dnkeylist.add("d3c6bfe8-ee9d-46d6-ba19-a17ad90a348f");
			
			NameNodeManagerFactory.getNameNodeManagerInstance()
					.cleanPartition(
							2007,12,"1","2");
		} catch (NameNodeManagerException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testlistPartitionDN(){
		try {
			ArrayList<String> dnkeylist = null;
			dnkeylist = NameNodeManagerFactory.getNameNodeManagerInstance()
					.listPartitionDN(
							2007,12,"1","2");
		} catch (NameNodeManagerException e) {
			e.printStackTrace();
			fail();
		}
	}

	public void testDeletePartition(){
		try {
			ArrayList<String> dnkeylist = new ArrayList<String>();
			dnkeylist.add("d3c6bfe8-ee9d-46d6-ba19-a17ad90a348f");
			ArrayList<Long> idlist = new ArrayList<Long>();
			idlist.add(1L);
			idlist.add(2L);
			idlist.add(3L);
			int version =  NameNodeManagerFactory.getNameNodeManagerInstance()
					.logPartitionDeleteOperation(
							2007,12,"1","2",idlist,true);

			NameNodeManagerFactory.getNameNodeManagerInstance()
					.finishPartitionDelete(2007, 12, "1", "2", version,
							dnkeylist);
		} catch (NameNodeManagerException e) {
			e.printStackTrace();
			fail();
		}
	}
}
