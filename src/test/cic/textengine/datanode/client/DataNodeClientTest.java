package test.cic.textengine.datanode.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.codec.DecoderException;
import org.junit.Test;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.RemoteTEItemWriter;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemWriterException;
import com.cic.textengine.repository.datanode.client.response.PingResponse;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class DataNodeClientTest {

	public void testPing() {
		DataNodeClient client = new DataNodeClient("192.168.1.12",6767);
		try {
			PingResponse response = client.ping();
			System.out.println(response.getErrorCode());
			System.out.println(response.getUpTime());

			
			assertNotNull(response);
		} catch (DataNodeClientException e) {
			e.printStackTrace();
			fail();
		} catch (DataNodeClientCommunicationException e) {
			e.printStackTrace();
			fail();
		}
	}

//	@Test
	public void testStreamWriteTEItem(){
		DataNodeClient client = new DataNodeClient("192.168.1.12",6767);
		int year, month;
		String siteid, forumid;
		year = 2008;
		month = 1;
		siteid = "1";
		forumid = "2";
		
		RemoteTEItemWriter writer;
		try {
			writer = client.getWriter(year, month, siteid, forumid);
			writer.writeTEItem(getSampleItem());
			writer.flush();
			writer.close();
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
	@Test
	public void testGetItems()
	{
		
		NameNodeClient nn_client = new NameNodeClient("192.168.1.158", 6868);
		int itemcount = 100;
		int year = 2008;
		int month = 2;
		String siteid = "FF111";
		String forumid = "3";
		
//		NameNodeClient nn_client = new NameNodeClient("192.168.2.2", 6869);
//		int itemcount = 1000;
//		int year = 2006;
//		int month = 12;
//		String siteid = "FF3";
//		String forumid = "Claritin_Search";

		
		try {
			DataNodeClient dn_client = nn_client.getDNClientForQuery(year, month, siteid, forumid);
			long current = System.currentTimeMillis();
			for(long id=0; id<itemcount; id++)
			{
				dn_client.queryItem(year, month, siteid, forumid, id);
			}
//			TEItem item = dn_client.queryItem(year, month, siteid, forumid, 0);
			current = System.currentTimeMillis() - current;
			System.out.println(String.format("It takes %s ms to query %s items.", current, itemcount));
			
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DataNodeClientCommunicationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DataNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			DataNodeClient dn_client = nn_client.getDNClientForQuery(year, month, siteid, forumid);
			long current = System.currentTimeMillis();
			for(long i=0; i<itemcount; i++)
			{
				ArrayList<Long> id = new ArrayList<Long>();
				id.add(i);
				ArrayList<TEItem> itemlist = dn_client.queryItems(year, month, siteid, forumid, id, true);
			}
			current = System.currentTimeMillis() - current;
			System.out.println(String.format("It takes %s ms to query %s items.", current, itemcount));
		} catch (NameNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DataNodeClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DataNodeClientCommunicationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

			int count = 0;
			DataNodeClient dn_client;
			try {
				dn_client = nn_client.getDNClientForQuery(year, month, siteid, forumid);
				RemoteTEItemEnumerator enu = dn_client.getItemEnumerator(year, month, siteid, forumid, 1, itemcount, false);
				long current = System.currentTimeMillis();
				while(enu.next() && count < itemcount)
				{
					TEItem item = enu.getItem();
					count ++;
				}
				enu.close();
				current = System.currentTimeMillis() - current;
				System.out.println(String.format("It takes %s ms to query %s items.", current, itemcount));
			} catch (NameNodeClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DataNodeClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DataNodeClientCommunicationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


	}
	
	private ArrayList<ItemKey> readItemKeys(String keyfile)
	{
		ArrayList<ItemKey> keylist = new ArrayList<ItemKey>();
		FileReader fr;
		try {
			fr = new FileReader(keyfile);
			BufferedReader br = new BufferedReader(fr);
			String itemkey = null;
			
			while((itemkey = br.readLine())!=null)
			{
				ItemKey key = ItemKey.decodeKey(itemkey);
				keylist.add(key);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return keylist;
	}
}
