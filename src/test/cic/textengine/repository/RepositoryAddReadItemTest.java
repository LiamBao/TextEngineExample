package test.cic.textengine.repository;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.io.*;
import org.apache.log4j.Logger;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.RemoteTEItemWriter;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemWriterException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.type.TEItem;

import test.cic.textengine.idf.*;

import junit.framework.TestCase;
/**
 * Purpose: to test the following methods
 *              com.cic.textengine.repository.datanode.client.RemoteTEItemWriter.writeTEItem(TEItem item)
 *              com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator.next()
 *              com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator.getItem()
 *              com.cic.textengine.repository.datanode.client.DataNodeClient.queryItems(int year, int month, String siteid, String forumid, ArrayList<Long> itemid_list, boolean sorted)
 * General parameters:   
 *              String fileName; //set the txt file path which stores the item.getMeta().getDateOfPost()
 *              long maxAddItem; //set the amount of items which write to TextEngine
 *              String host; // set the NameNodeClient host
 *              int port; //set the NameNodeClient port
 *              int year;int month;String siteid;String forumid;//set the partition key
 * TestCases: 
 *              TestCase Name      |           Usage                      |      Parameters
 *              -------------------------------|---------------------------------------------------|---------------------------------------------------------
 *              testAddItemRemote()            |add local item to TextEngine                       |
 *              -------------------------------|---------------------------------------------------|---------------------------------------------------------
 *              testEnumerateItems()           |test RemoteTEItemEnumerator                        |
 *              -------------------------------|---------------------------------------------------|---------------------------------------------------------
 *              testQueryItems()               |test DataNodeClient.queryItems()                   |
 *              -------------------------------|---------------------------------------------------|---------------------------------------------------------
 *  Steps:
 *      1. run testAddItemRemote()           
 *      2. run testEnumerateItems() and testQueryItems()
 *      
 * @author ellen
 *
 */
public class RepositoryAddReadItemTest extends TestCase {
	Logger log = Logger.getLogger(RepositoryAddReadItemTest.class);
	int year;
	int month;
	String siteid;
	String forumid;
	IDFEngineTest idfEngine;
	IDFEngineTest.SampleItems si;
	String fileName;
	long ts_start;
	long maxAddItem;
	long startItemID;
	final static int CLEAR_LIST=2000;
	String host;
	int port;
	RandomAccessFile raf;
	protected void setUp() throws Exception {
		super.setUp();
		year=2004;
		month=1;
		siteid="281";
		forumid="FID87_2FID311133s2";
		idfEngine=new IDFEngineTest();
		si=idfEngine.new SampleItems();
		fileName="/home/ellen/testdata3/write1/addItem1.txt";
		maxAddItem=10000;
		host="192.168.2.2";
		port=6868;
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void AtestAddItemRemote(){
		NameNodeClient client = new NameNodeClient(host,port);
		DataNodeClient dn_client = null;
		try {
		//	dn_client=client.getDNClientForAppending(year, month, siteid, forumid);
			dn_client = client.getDNClientForWriting(year, month, siteid, forumid);
			//dn_client = client.getDNClientForAppending(2007, 12, "1", "2");
		} catch (NameNodeClientException e) {
			
			e.printStackTrace();
			fail();
		}
		
		ts_start = System.currentTimeMillis();
		si.iniFile(fileName);
	
		RemoteTEItemWriter writer;
		try {
			RandomAccessFile raf=new RandomAccessFile(fileName,"rw");
			StringBuffer stringB=new StringBuffer();
		  for(int i=0;i<=100;i++)
			  stringB.append("*");
		  raf.writeBytes(stringB.toString());
		  raf.close();
			writer = dn_client.getWriter(year, month, siteid, forumid);
			for (int i = 1;i<=maxAddItem;i++){
				writer.writeTEItem(si.getSampleItem2(i, fileName));
				
			}
			writer.close();
			startItemID=writer.getStartItemID();
			assertTrue(maxAddItem==writer.getCount());
			
			
			StringBuffer tempS=new StringBuffer(new Long(startItemID).toString());
			int tempSL=tempS.length();
			if(tempSL<100)
			{
				for(int j=0;j<100-tempSL;j++)
				{
					tempS.append("*");
				}
			}
			raf=new RandomAccessFile(fileName,"rw");
			raf.seek(0l);
			raf.writeBytes(tempS+"\n");
			
			raf.close();
			log.info("startItemID="+startItemID);
			log.info("itemCount="+writer.getCount());
		} catch (DataNodeClientException e) {
		
			e.printStackTrace();
			fail();
		} catch (IOException e) {
		
			e.printStackTrace();
			fail();
		} catch (DataNodeClientCommunicationException e) {
		
			e.printStackTrace();
		} catch (RemoteTEItemWriterException e) {

			e.printStackTrace();
		}
	}
/*	public void AtestReader(){
		NameNodeClient client = new NameNodeClient("192.168.2.2",6868);
		DataNodeClient dn_client = null;
		try {
			dn_client = client.getDNClientForQuery(year, month, siteid, forumid);
			
		} catch (NameNodeClientException e) {
		
			e.printStackTrace();
			fail();
		}

		RemoteTEItemEnumerator enu;
		try {
			
			enu = dn_client.getItemEnumerator(year, month, siteid, forumid);
			
			while(enu.next()){
			
				TEItem item=enu.getItem();
				log.info(item.getMeta().getItemID());
			}
			enu.close();
			
		} catch (DataNodeClientException e) {
		
			e.printStackTrace();
		} catch (DataNodeClientCommunicationException e) {
		
			e.printStackTrace();
		} catch (IOException e) {
		
			e.printStackTrace();
		}
	}*/
	public void testEnumerateItems(){
		//TODO: Need to test the enumerate right after the addItem function finishs. The enumerate
		//should be able to read all the items which were added.
		
		 ts_start = System.currentTimeMillis();

		NameNodeClient client = new NameNodeClient(host,port);
		DataNodeClient dn_client = null;
		try {
			dn_client = client.getDNClientForQuery(year, month, siteid, forumid);
		} catch (NameNodeClientException e) {
		
			e.printStackTrace();
			fail();
		}

		RemoteTEItemEnumerator enu;
		try {
			raf=new RandomAccessFile(fileName,"rw");
			String tempS= raf.readLine();
			startItemID=new Long(tempS.substring(0, tempS.indexOf("*")));
			log.info("startItemID=="+startItemID);
			ArrayList<TEItem>item_list=new ArrayList<TEItem>();
			enu = dn_client.getItemEnumerator(year, month, siteid, forumid);
		long temp=0;
	
			while(enu.next()){
				temp++;
				if(temp>=startItemID)
			item_list.add(enu.getItem());
			 
			
				if(item_list.size()>CLEAR_LIST){
					
					assertTrue(si.isEquals(item_list, raf));
					item_list.clear();
				}
				
				
			
				
			}
			if(item_list.size()>0){
				assertTrue(si.isEquals(item_list, raf));
				item_list.clear();
			}
			
			
			enu.close();
			raf.close();
			
			assertTrue((temp-maxAddItem)==(startItemID-1));

			log.info("Time:" + si.format((System.currentTimeMillis() - ts_start)));
		} catch (DataNodeClientException e) {
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		} catch (DataNodeClientCommunicationException e) {
		
			e.printStackTrace();
		}
	}	
	
	public void testQueryItems(){
		long ts_start = System.currentTimeMillis();
		NameNodeClient client = new NameNodeClient(host,port);
		
		ArrayList<TEItem> result = null;
		DataNodeClient dn_client = null;
		try {
			raf=new RandomAccessFile(fileName,"rw");
			String tempS= raf.readLine();
			startItemID=new Long(tempS.substring(0, tempS.indexOf("*")));
			log.info("startItemID=="+startItemID);
			dn_client = client.getDNClientForQuery(year, month, siteid, forumid);
		

		java.util.ArrayList<Long> idlist = new ArrayList<Long>();
		long i=1;
		int total=0;
		log.info("Start...");
		while(i<=(startItemID+maxAddItem-1))
		{
			if(i>=startItemID)
			idlist.add(i);
			
			
			
			if(idlist.size()>CLEAR_LIST)
			{
				//log.info("begin query");
				result = dn_client.queryItems(year, month, siteid, forumid, idlist, true);
				total+=result.size();
				assertTrue(si.isEquals(result, raf));
				
				result.clear();
				idlist.clear();
					
					
				//	assertTrue(si.isEquals(result, raf));
				
			}
		    i++;
		}
		if(idlist.size()>0)
		{
			
			result = dn_client.queryItems(year, month, siteid, forumid, idlist, true);
			total+=result.size();
			assertTrue(si.isEquals(result, raf));
			
		
		}
		assertTrue(total==maxAddItem);
		raf.close();
		log.info("total query time="+si.format(System.currentTimeMillis()-ts_start));
		
		} catch (NameNodeClientException e) {
		
			e.printStackTrace();
			fail();
		}  catch (DataNodeClientException e) {
		
			e.printStackTrace();
			fail();
		}   catch(IOException e){
			e.printStackTrace();
		} catch (DataNodeClientCommunicationException e) {
		
			e.printStackTrace();
		}
		
		
	}
}
