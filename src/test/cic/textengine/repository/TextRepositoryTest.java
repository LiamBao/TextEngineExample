package test.cic.textengine.repository;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.RemoteTEItemWriter;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemWriterException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class TextRepositoryTest {
	int year = 2007;
	int month = 3;
	String siteid = "FF200";
	String forumid = "13";
	String NNDaemonAddress = "192.168.1.158";
	int NNDaemonPort = 6868;
	
	@Test
	public void testEnumerateItems(){

		long ts_start = System.currentTimeMillis();

		NameNodeClient client = new NameNodeClient(NNDaemonAddress,NNDaemonPort);
		DataNodeClient dn_client = null;

		try {
			dn_client = client.getDNClientForQuery(year, month, siteid, forumid);
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}

		RemoteTEItemEnumerator enu;
		try {
			enu = dn_client.getItemEnumerator(year, month, siteid, forumid);
			while(enu.next()){
				System.out.println(enu.getItem().getMeta().getItemID());
				
			}
			enu.close();

			System.out.println("Time:" + (System.currentTimeMillis() - ts_start) + " ms");
		} catch (DataNodeClientException e) {
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		} catch (DataNodeClientCommunicationException e) {
			e.printStackTrace();
			fail();
		}
	}	
	
	public void testQueryItems(){
		long ts_start = System.currentTimeMillis();
		NameNodeClient client = new NameNodeClient(NNDaemonAddress,NNDaemonPort);
		DataNodeClient dn_client = null;
		try {
			dn_client = client.getDNClientForQuery(year, month, siteid, forumid);
			System.out.println("DN Client Retrieved:" + dn_client.getHost());
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}

		java.util.ArrayList<Long> idlist = new ArrayList<Long>();
		idlist.add(10L);
		idlist.add(20L);
		ArrayList<TEItem> result = null;
		try {
			result = dn_client.queryItems(year, month, siteid, forumid, idlist, true);
			
			for (int i = 0;i<result.size();i++){
				System.out.println(result.get(i).getMeta().getItemID());
			}
			System.out.println("Time:" + (System.currentTimeMillis() - ts_start) + " ms");
		} catch (DataNodeClientException e1) {
			e1.printStackTrace();
			fail();
		} catch (DataNodeClientCommunicationException e) {
			e.printStackTrace();
			fail();
		}
	}
	
	public void testDeleteItems(){
		NameNodeClient client = new NameNodeClient(NNDaemonAddress,NNDaemonPort);
		DataNodeClient dn_client = null;
		try {
			dn_client = client.getDNClientForWriting(year, month, siteid, forumid);
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		long ts_start = System.currentTimeMillis();
		
		try {
			ArrayList<Long> idlist = new ArrayList<Long>();
			idlist.add(10L);
			idlist.add(11L);
			idlist.add(12L);
			idlist.add(13L);
			idlist.add(14L);
			dn_client.deleteItems(year, month, siteid, forumid, idlist, true);
			
			System.out.println("Time:" + (System.currentTimeMillis() - ts_start) + " ms");
		} catch (DataNodeClientException e) {
			e.printStackTrace();
			fail();
		} catch (DataNodeClientCommunicationException e) {
			e.printStackTrace();
			fail();
		} 	
	}

	public void testAddItems(){
		NameNodeClient client = new NameNodeClient(NNDaemonAddress,NNDaemonPort);
		
		DataNodeClient dn_client = null;
		try {
			dn_client = client.getDNClientForWriting(year, month, siteid, forumid);
			System.out.println("DNClient retrieved for adding:" + dn_client.getHost());
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		
		long ts_start = System.currentTimeMillis();
		
		RemoteTEItemWriter writer;
		try {
			writer = dn_client.getWriter(year, month, siteid, forumid);
			for (int i = 0;i<1000000;i++){
				writer.writeTEItem(getSampleItem());
			}
			writer.close();
			
			System.out.println("Start Item ID:" + writer.getStartItemID()
					+ ", count:" + writer.getCount());
			System.out.println("Time:" + (System.currentTimeMillis() - ts_start) + " ms");
		} catch (DataNodeClientException e) {
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		} catch (DataNodeClientCommunicationException e) {
			e.printStackTrace();
			fail();
		} catch (RemoteTEItemWriterException e) {
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
