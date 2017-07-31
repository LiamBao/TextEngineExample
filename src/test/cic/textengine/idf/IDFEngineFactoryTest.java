package test.cic.textengine.idf;

import java.io.File;
import java.io.IOException;

import com.cic.data.Item;
import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.IDFEngineFactory;
import com.cic.textengine.idf.IDFReader;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;
import com.cic.textengine.idgenarator.IDGenerator;
import com.cic.textengine.itemreader.ItemReader;
import com.cic.textengine.itemreader.XMLItemReader;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

import java.util.ArrayList;

import junit.framework.TestCase;

public class IDFEngineFactoryTest extends TestCase {
	
	public void AtestWriteIDFEngine(){

		IDGenerator.getInstance(0);
		
		ItemReader reader = null;
		try {
			reader = new XMLItemReader("c:\\temp\\20771022\\test");
		} catch (Exception e1) {
			
			e1.printStackTrace();
			fail();
			return;
		}

		File file = new File("d:\\test.idf");
		IDFEngine engine = null;
		try {
			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
			fail();
			return;
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
			return;
		}

		long ts_start = System.currentTimeMillis();
		int count = 0;
		ArrayList<TEItem> list = null;
		list = new ArrayList<TEItem>();
		try {
			while (reader.next()) {
				Item item = reader.getItem();
				list.add((TEItem)item);
				count++;
				if (list.size()>=100){
					engine.appendItems(list);
					System.out.println(count + " items are written to the IDF.");
					list = new ArrayList<TEItem>();
				}
			}
			if (list.size()>0){
				engine.appendItems(list);
				System.out.println(count + " items are written to the IDF.");
			}
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		long ts_end = System.currentTimeMillis();
		System.out.println("Total MS ellapsed:" + (ts_end - ts_start));
		System.out.println("Average: " + ((ts_end - ts_start)/count) + " each item");
	}

	public void AtestQueryIDFEngine(){
		File file = new File("d:\\test.idf");
		IDFEngine engine = null;
		try {
			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
			TEItem item = engine.getItem(2);
			System.out.println(item.getMeta().getDateOfPost());
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
			fail();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
		}		
	}
	
	public void AtestReadIDFEngine(){
		File file = new File("d:\\test.idf");
		IDFEngine engine = null;
		try {
			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
			fail();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
		}

		try {
			System.out.println("Count in IDF:" + engine.getItemCount());
			long ts_start = System.currentTimeMillis();
			TEItem item = engine.getItem(550);
			long ts_end = System.currentTimeMillis();
			System.out.println("Date Of Post:" + item.getMeta().getDateOfPost());
			
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
		}
	}
	
	public void testIDFReader(){
		File file = new File("/home/joe.sun/200808_4646333439_464944626d77352e69464944_0.idf");
		IDFEngine engine = null;
		try {
			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
			IDFReader reader = engine.getIDFReader(0,true);
			int count = 0;
			while(reader.next()){
				count++;
				System.out.println(reader.getItem().getSubject());
			}
			System.out.println("Count:" + count);
			reader.close();
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
			fail();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public void AtestInitIDFEngine(){
		File file = new File("d:\\test.idf");
		IDFEngine engine = null;
		try {
			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
			fail();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
		}
	}
	
	public void AtestDelete(){
		File file = new File("d:\\test.idf");
		IDFEngine engine = null;
			try {
				engine = IDFEngineFactory.getNewIDFEngineInstance(file);
				ArrayList<Integer> idlist = new ArrayList<Integer>();
				idlist.add(1);
				engine.deleteItems(idlist, true);
			} catch (IDFEngineInitException e) {
				
				e.printStackTrace();
			} catch (IDFEngineException e) {
				
				e.printStackTrace();
			}

	}
	
	public void AtestAppendIDFEngine(){
		File file = new File("d:\\test_twin.idf");
		IDFEngine engine = null;
		try {
			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
			ArrayList<TEItem> list = new ArrayList<TEItem>();
			TEItem item = null;
			long start_time = System.currentTimeMillis();
			for (int i = 0 ;i<5000;i++){
				item = getSampleItem();
				item.getMeta().setDateOfPost(System.currentTimeMillis());
				//System.out.println(i + ":" + item.getMeta().getDateOfPost());
				list.add(item);
			}
			engine.addItems(list,4001);
			System.out.println("Time(ms):" + (System.currentTimeMillis() - start_time));
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
			fail();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			
			e.printStackTrace();
			fail();
		}
	}
	
	public void AtestAddIDFEngine(){
		File file = new File("d:\\test.idf");
		IDFEngine engine = null;
		try {
			engine = IDFEngineFactory.getNewIDFEngineInstance(file);
			ArrayList<TEItem> list = new ArrayList<TEItem>();
			
			TEItem item = null;
			long ts;
			ts = System.currentTimeMillis();
			for (int i = 0;i<1000;i++){
				item = getSampleItem();
				item.getMeta().setDateOfPost(ts);
				ts++;
				list.add(item);
			}

			int startItemID = 1;
			for (int i = 0;i<20;i++){
				engine.addItems(list,startItemID);
				startItemID += list.size() - 500;
			}
			
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
			fail();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			
			e.printStackTrace();
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
