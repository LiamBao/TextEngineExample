package test.cic.textengine.type;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.codec.DecoderException;
import org.junit.Test;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.type.TEItem;

public class ItemKeyTest {
	
	@Test
	public void testItemKey()
	{
		String source = "FF";
		String siteid = "349";
		String forumid = "FID3232FID";
		int year = 2008;
		int month = 7;
		long itemid = 4563;
		System.out.println(String.format("Souece:%s, SiteID:%s, ForumID:%s, Year:%s, Month:%s, ItemID:%s", 
				source, siteid, forumid, year, month, itemid));
		ItemKey itemKey = new ItemKey(source, siteid, forumid, year, month, itemid);
		String key = itemKey.generateKey();
		System.out.println("The generated itemkey is: "+key);
		try {
			ItemKey reKey = ItemKey.decodeKey(key);
			System.out.println(String.format("Souece:%s, SiteID:%s, ForumID:%s, Year:%s, Month:%s, ItemID:%s", 
					reKey.getSource(), reKey.getSiteID(), reKey.getForumID(), reKey.getYear(), reKey.getMonth(), reKey.getItemID()));
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGenerateItemkey()
	{
		String source = "FF";
		String siteid = "234";
		String forumid = "FID39FID";
		int year = 2008;
		int month = 1;
		long itemid = 1000;
		
		NameNodeClient nn_client = new NameNodeClient("192.168.1.158", 6868);
		try {
			FileWriter fw = new FileWriter("/home/joe.sun/Desktop/itemkey.list");
			PrintWriter pw = new PrintWriter(fw);
			try {
				DataNodeClient dn_client = nn_client.getDNClientForQuery(year, month, source+siteid, forumid);
				System.out.println("DN node ip:"+dn_client.getHost());
				RemoteTEItemEnumerator enu = dn_client.getItemEnumerator(year, month, source+siteid, forumid, 1, itemid, false);
//				RemoteTEItemEnumerator enu = dn_client.getItemEnumerator(year, month, siteid, forumid);
				while(enu.next())
				{
					TEItem item = enu.getItem();
					ItemKey itemkey = new ItemKey(item);
					pw.println(itemkey.generateKey());
				}
				enu.close();
				
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
			
			pw.close();
			fw.close();
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}
