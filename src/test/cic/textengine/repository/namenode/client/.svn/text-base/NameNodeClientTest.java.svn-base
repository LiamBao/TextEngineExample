package test.cic.textengine.repository.namenode.client;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;

public class NameNodeClientTest {

	public void AtestApplyPartitionWriteLock() {
		fail("Not yet implemented");
	}

	public void testGetDNClientForAppending() {
		NameNodeClient client = new NameNodeClient("192.168.2.2",6869);
		try {
			int year = 2008;
			int month = 6;
			String siteid = "FF349";
			String forumid = "FIDxiali.iFID";
			DataNodeClient ddclient = client.getDNClientForWriting(year, month, siteid, forumid);
			long itemcount = client.getDNPartitionItemCount(ddclient.getDNKey(), year, month, siteid, forumid);
			
			System.out.println(String.format("There are %s items in this partition.", itemcount));
			
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
	}

	public void AtestReleasePartitionWriteLock() {
		fail("Not yet implemented");
	}

	public void AtestUpateDataNodeStatus() {
		fail("Not yet implemented");
	}

	public void AtestValidateDataNode() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testCleanNameNodeCache(){
		NameNodeClient client = new NameNodeClient("192.168.3.70",6868);
		try {
			client.cleanCache();
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
