package test.cic.textengine.idf;


import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import junit.framework.TestCase;


import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.IDFEnginePool;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;
import com.cic.textengine.idf.*;

import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

import java.io.*;
import java.util.ArrayList;
import java.math.BigInteger;

/**
 * 
 * @author ellen
 *
 *  Purpose: to test the following methods 
 *              com.cic.textengine.idf.IDFEngine.appendItems(ArrayList<TEItem> items)
 *              com.cic.textengine.idf.IDFEngine.addItems(ArrayList<TEItem> items, int start_idx)
 *              com.cic.textengine.idf.IDFEngine.getItems(ArrayList<Integer> index_list, boolean sorted)
 *              com.cic.textengine.idf.IDFEngine.getItem(int itemIndex)
 *  General parameters: 
 *              @param File file           the IDF file to store TEItem
 *              @param String fileName     the txt file to store TEItem.getMeta.getDateOfPost() string 
 *              @param SampleItems si      the InnerClass to generate TEItem, append items, etc.    
 *  TestCases:   
 *                   TestCase Name      |           Usage                      |      Parameters
 *                   -------------------------------- |--------------------------------------------------|---------------------------------------
 *              1.testAppendIDFEngine() | append the expected amount of        |   int expectedAmount
 *                                      |    TEItems in an IDF file            |
 *             --------------------------------------|---------------------------------------------------|--------------------------------------
 *              2.testAppendLastIDF()   |fulfill the IDF file to maximum amount|
 *              -------------------------------------|---------------------------------------------------|----------------------------------------
 *              3. testAddIDF()         |add the expected amount of TEItems to |   int expectedAmount
 *                                      |a specified position in the IDF file  |   int expectedPos
 *         ------------------------------------------|---------------------------------------------------|---------------------------------------
 *             4.  testAddAllIDF()      |fulfill the IDF file to maximum amount|   int testAddAllIDF()
 *                                      |     from a specified position        |
 *          -----------------------------------------|---------------------------------------------------|--------------------------------------
 *            5. testAddLoopIDFEngine() |add items in loop to the IDF file     | int loopAmount //the biggest amount
 *                                      |  (Overlap adding)                    | int extend // the extend between min and max
 *                                      |                                      | notice: loopAmount should not less than extend
 *          -----------------------------------------|---------------------------------------------------|--------------------------------------
 *                6. testReadItems()    |  Read Files from start to end,test   |notice: this method can run only if the idf file  
 *                                      |           engine.getItems()          |        is added from the 1st position
 *                                      |                                      |example: run testAddIDF() or testAddAllIDF() by 
 *                                      |                                      |         set expectedPos=1
 *                       ----------------------------|----------------------------------------------------|--------------------------------------
 *                                                         
 */
public class IDFEngineTest extends TestCase{
	Logger log = Logger.getLogger(IDFEngineTest.class);
	File file=null;
	String folder;
	String fileName=null;
	IDFEngine engine = null;
	SampleItems si=null;
	int expectedAmount;
	int expectedPos;
	final static int CLEAR_LIST_AMOUNT=2000;
	int loopAmount;
	int extend;
	@Before
	public void setUp() throws Exception {
		folder="/home/ellen/testdata2/";
		file=new File(folder+"test.idf");
		fileName=(folder+"writePartition.txt");
		engine = IDFEnginePool.getInstance().getIDFEngineInstance(file);
		si=new SampleItems();
		//set at testAppendIDFEngine(),testAddIDF()
		expectedAmount=2000;
		//testAddIDF(), testAddAllIDF(),the expected position
		expectedPos=1;
		//testAddLoopIDFEngine
		loopAmount=401; //loopAmount should be larger than extend
		extend=200;
	}

	@After
	public void tearDown() throws Exception {
	}
	
	public void AtestAppendIDFEngine(){
		
		    int currentItemCount=engine.getItemCount();
		   
		    long startT=System.currentTimeMillis();
		    
			si.appendSampleItems(engine,expectedAmount,fileName);
			long processT=System.currentTimeMillis()-startT;
		
			int totalAppendItemCount=engine.getItemCount()-currentItemCount;
			log.info("total time="+si.format(processT)+" average Time="+si.format(processT/totalAppendItemCount));
			//assert the append amount is equal with the expected
	        assertTrue(expectedAmount==totalAppendItemCount);
	        
	        //assert every TEItem value was inserted correctly
		       assertTrue(si.isEqual(engine, fileName, currentItemCount+1));
		}
	
	public void testAppendLastIDF(){
	      int currentItemCount=engine.getItemCount();
			int leftItems=engine.getMaxItemCount()-currentItemCount;
			si.appendSampleItems(engine, leftItems, fileName);
			//assert the engine's item count has achieved the maximum
			assertTrue(engine.getItemCount()==engine.getMaxItemCount());
			//assert the engine's new items inserted correctly
			assertTrue(si.isEqual(engine, fileName, (currentItemCount+1)));
		}
	/**
	 * add items at the specified position
	 */
	public void testAddIDF(){
	//	int currentItemCount=engine.getItemCount();
		si.addSampleItems(engine, expectedPos, expectedAmount, fileName);
		//assert the engine's item count is correct
		log.info(engine.getItemCount()+" "+(expectedPos+expectedAmount-1));
		assertTrue(engine.getItemCount()==(expectedPos+expectedAmount-1));
		//assert the engine's new items inserted correctly
		assertTrue(si.isEqual(engine, fileName, expectedPos));
	}
	
	public void AtestAddAllIDF(){
	
			log.info("Start....");
			long startT=System.currentTimeMillis();
			si.addAllSampleItems(engine,fileName,expectedPos);
	     long processT=System.currentTimeMillis()-startT;
	     log.info("The total process time= "+si.format(processT));
	     assertTrue(engine.getItemCount()==engine.getMaxItemCount());
	      assertTrue(si.isEqual(engine, fileName, expectedPos));
	  
	     
		
	
	}
	
	
	
	public void AtestIsEqual(){
		si.isEqual(engine, fileName, 1);
	}
	public void AtestIDFReader(){
		try {
			IDFReader reader=engine.getIDFReader();
			RandomAccessFile raf=new RandomAccessFile(fileName,"rw");
			raf.seek(0);
			while(reader.next())
			{
				TEItem item=reader.getItem();
				String tempS=new Long(item.getMeta().getDateOfPost()).toString();
				
				String tempF=raf.readLine();
				tempF=tempF.substring(tempF.indexOf(":")+1, tempF.indexOf("*"));
				log.info(tempF+" / "+tempS);
				assertTrue(tempF.equals(tempS));
			}
			reader.close();
			raf.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		} 
		
	}
	public void AtestReadItems(){
	
		try {
		
			ArrayList<TEItem> item_list=new ArrayList<TEItem>();
			ArrayList<Integer>index_list=new ArrayList<Integer>();
			SampleItems si=new SampleItems();
			BigInteger i=new BigInteger("1");
			BigInteger max=new BigInteger(new Integer(engine.getItemCount()).toString());
			
			int temp=0;
			BigInteger addI=new BigInteger("1");
			max=max.add(addI);
			RandomAccessFile myFileStream = new RandomAccessFile(fileName,"rw");
			boolean sorted=true;
			myFileStream.seek(0);
			
			while(!i.equals(max))
			{
				index_list.add(i.intValue());
				i=i.add(addI);
				temp+=1;
				
				
				if(temp==CLEAR_LIST_AMOUNT){
				
					item_list=engine.getItems(index_list, sorted);
					log.info("item_list size="+item_list.size()+" index_list size="+index_list.size());
					for(int j=0;j<item_list.size();j++)
					{
						log.info((j+1)+"="+new Long(item_list.get(j).getMeta().getDateOfPost()).toString());
					}
					/*if(!si.isEquals(item_list, myFileStream))
					{
						log.info("the error position="+i.intValue()+": "+engine.getItem(i.intValue()).getMeta().getDateOfPost());
						log.info("write error");
						break;
					}*/
					assertTrue(si.isEquals(item_list, myFileStream));
					
					index_list.clear();
					temp=0;
				}
			
				
				
			}
			if(index_list.size()>0)
			{
				item_list=engine.getItems(index_list, sorted);
				log.info("item_list size="+item_list.size()+" index_list size="+index_list.size());
				/*if(!si.isEquals(item_list, myFileStream))
				{
					log.info("the error position="+i.intValue()+": "+engine.getItem(i.intValue()).getMeta().getDateOfPost());
					log.info("write error");
					
				}*/
				assertTrue(si.isEquals(item_list, myFileStream));
				index_list.clear();
			}
			myFileStream.close();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public void AtestCompare2IDF()
	{
		File folderPath=new File("/home/ellen/testdata5/");
		File[] files = folderPath.listFiles(new FilenameFilter(){
			public boolean accept(File dir,String name){
				if (name.toLowerCase().endsWith(".idf")){
					return true;
				}else{
					return false;
				}
			}
		});
		
		String name="200401_31_4649443532464944_0.idf";
		File file1=new File("/home/ellen/testdata4/IDF/"+name);
		File file2=new File("/home/ellen/testdata5/"+name);
		try {
			IDFEngine engine1 = IDFEnginePool.getInstance().getIDFEngineInstance(file1);
			IDFEngine engine2=IDFEnginePool.getInstance().getIDFEngineInstance(file2);
			assertTrue(engine1.getItemCount()==engine2.getItemCount());
			IDFReader reader1=engine1.getIDFReader();
			IDFReader reader2=engine2.getIDFReader();
			int count=0;
			while(reader1.next()&&reader2.next()){
				TEItem item1=reader1.getItem();
				TEItem item2=reader2.getItem();
				count++;
				log.info(item1.getMeta().getItemID()+"/"+item2.getMeta().getItemID());
				assertTrue(item1.getMeta().getDateOfPost()==item2.getMeta().getDateOfPost());
			}
			log.info(count);
			log.info(engine1.getItemCount());
			log.info("file size"+files.length+" "+files[0].getName());
		} catch (IDFEngineInitException e) {
			
			e.printStackTrace();
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	public void AtestAddLoopIDFEngine(){
	
	
		int min=1;
	
		
		int max=min+extend;
		int mid=0;
		log.info("Start....");
		long startT=System.currentTimeMillis();
		try {
			RandomAccessFile raf=new RandomAccessFile(fileName,"rw");
			raf.setLength(0);
			raf.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		while(max<loopAmount){
		si.addLoopSampleItems(engine, min, max,fileName);
		log.info("before min="+min+"max="+max);
		min=(min+max)/2;
		max=min+extend;
		mid=(min+max)/2;
		log.info("min="+min+"max="+max);
		
		}
	//	assertTrue(si.isEqualAdd(engine, fileName, extend));
        assertTrue(si.isEqualAdd2(engine, fileName, extend));
		
		log.info("Total time="+si.format(System.currentTimeMillis()-startT));
		
	}
	
	
	public class SampleItems
	{
		public void appendSampleItems(IDFEngine engine,int amount,String fileName)
		{
			
			ArrayList<TEItem> list = new ArrayList<TEItem>();
			BigInteger i=new BigInteger("0");
			BigInteger max;
			if(amount<1)
		     {
				max=new BigInteger((new Integer(engine.getMaxItemCount())).toString());
		     }
		
			else{
			   max=new BigInteger(new Integer(amount).toString());
			}
			try {
				
				long startE=System.currentTimeMillis();
				RandomAccessFile raf=new RandomAccessFile(fileName,"rw");
				raf.setLength(0);
				raf.close();
			while(!i.equals(max))
			{
				
			    i=i.add(new BigInteger("1"));
			    list.add(getSampleItem2(i.intValue(),fileName));
			//    log.info("i="+i+"  max="+max);
			    	if(list.size()>CLEAR_LIST_AMOUNT)
			    	{	
			    		clearAppendList(engine,list);
			    		
					//log.info(engine.getItemCount()+"="+engine.getItem(engine.getItemCount()).getMeta().getDateOfPost());
					
			    	}
			
			   
			}
			if(list.size()>0)
			{
				clearAppendList(engine,list);
				
			//	log.info(engine.getItemCount()+"="+engine.getItem(engine.getItemCount()).getMeta().getDateOfPost());
				
				
			}
			long processWh=System.currentTimeMillis()-startE;
			log.info("The whole append sample time is: "+format(processWh));
			}  catch(IOException e){
				e.printStackTrace();
			}
			
		}
		void clearAppendList(IDFEngine engine,ArrayList<TEItem> list)
		{
			try {
				try {
					
					engine.appendItems(list);
				
					list.clear();
				} catch (IDFEngineException e) {
					
					e.printStackTrace();
				}
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		}
		
		void addAllSampleItems(IDFEngine engine,String fileName,int pos)
		{
			ArrayList<TEItem> list = new ArrayList<TEItem>();
			BigInteger i=new BigInteger(new Integer(pos).toString());
			BigInteger max=new BigInteger((new Integer(engine.getMaxItemCount())).toString());
			//BigInteger max=new BigInteger("10000");
			RandomAccessFile raf;
			try {
				raf = new RandomAccessFile(fileName,"rw");
				raf.setLength(0);
				raf.close();
			} catch (IOException e1) {
				
				e1.printStackTrace();
			}
			
			while(!(i.subtract(max).intValue()>0))
			{
				
			    i=i.add(new BigInteger("1"));
			    TEItem item=getSampleItem2(i.intValue(),fileName);
			 //    log.info("new TEItem "+i.intValue()+": "+item.getContent());
			    list.add(item);
			    
			    if(list.size()>CLEAR_LIST_AMOUNT)
			    {
			    	clearList(engine,new BigInteger(new Integer(pos).toString()),list);
			    	pos=engine.getItemCount()+1;
					} 
					
			    }
			if(list.size()>0)
			{
				BigInteger temp=new BigInteger(new Integer(engine.getItemCount()+1).toString());
				log.info("the insert index is "+temp);
				clearList(engine,temp,list);
			   
			}
			try {
				log.info("1= "+engine.getItem(1).getMeta().getDateOfPost());
				log.info(engine.getItemCount()+"="+engine.getItem(engine.getItemCount()).getMeta().getDateOfPost());
			} catch (IDFEngineException e) {
				
				e.printStackTrace();
			}
			
		}
			
		/**
		 * add specified amount items in the engine
		 * @param engine
		 * @param pos   the inserted position
		 * @param amount the amount of inserted items
		 */
	   void addSampleItems(IDFEngine engine,int pos,int amount,String fileName){
		   ArrayList<TEItem>item_list=new ArrayList<TEItem>();
		  
		  RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(fileName,"rw");
			 raf.setLength(0);
			 raf.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		 
		    for(int i=1;i<=amount;i++){
		    	
		    	item_list.add(getSampleItem2(i,fileName));
		    	if(item_list.size()>CLEAR_LIST_AMOUNT)
		    	{
		    		clearList(engine,new BigInteger(new Integer(pos).toString()),item_list);
		    		pos=engine.getItemCount()+1;
		    	}
		    }
		    if(item_list.size()>0)
	    	{
	    		clearList(engine,new BigInteger(new Integer(pos).toString()),item_list);
	    	}
		    
	   }
		
		
		void addLoopSampleItems(IDFEngine engine,int start,int maxI,String fileName){
			ArrayList<TEItem> list = new ArrayList<TEItem>();
			BigInteger i=new BigInteger(new Integer(start).toString());
			BigInteger is=new BigInteger(new Integer(start).toString());
			BigInteger max=new BigInteger(new Integer(maxI+1).toString());
		
			
			while(!i.equals(max)){
				list.add(getSampleItem2(i.intValue(),fileName));
				 if(list.size()>CLEAR_LIST_AMOUNT)
				    {
				    	clearList(engine,is,list);
				    	is=is.add(new BigInteger(new Integer(engine.getItemCount()+1).toString()));
				    }
				   i=i.add(new BigInteger("1"));
			}
			if(list.size()>0)
			clearList(engine,is,list);
			
			
			
			
			
		}
		
		void clearList(IDFEngine engine,BigInteger i,ArrayList<TEItem> list)
		{
			try {
				//engine.appendItems(list);
				log.info(engine.getItemCount()+" start="+i.intValue());
				long startTime=System.currentTimeMillis();
				try {
					engine.addItems(list, i.intValue());
				} catch (IOException e) {
					
					e.printStackTrace();
				}
				long processTime=System.currentTimeMillis()-startTime;
				
				log.info("processTime="+format(processTime));
			
			} catch (IDFEngineException e) {
				
				e.printStackTrace();
			}
			list.clear();
		}
		
		
		public  String format(long ms){
			int ss=1000;
			int mi=ss*60;
			int hh=mi*60;
			int dd=hh*24;
			
			long day=ms/dd;
			long hour = (ms - day * dd) / hh;
			   long minute = (ms - day * dd - hour * hh) / mi;
			   long second = (ms - day * dd - hour * hh - minute * mi) / ss;
			   long milliSecond = ms - day * dd - hour * hh - minute * mi - second * ss;

			   String strDay = day < 10 ? "0" + day : "" + day;
			   String strHour = hour < 10 ? "0" + hour : "" + hour;
			   String strMinute = minute < 10 ? "0" + minute : "" + minute;
			   String strSecond = second < 10 ? "0" + second : "" + second;
			   String strMilliSecond = milliSecond < 10 ? "0" + milliSecond :""+milliSecond;
			   strMilliSecond = milliSecond < 100 ? "0" + strMilliSecond : "" + strMilliSecond;
			   return strDay + " " + strHour + ":" + strMinute + ":" + strSecond + " " + strMilliSecond;
			   
		}
		public TEItem getSampleItem2(int i,String fileName)
		{
			TEItem item = new TEItem();
			String timeS=new Long(System.currentTimeMillis()).toString();
			int intS=new Integer(timeS.substring(timeS.length()-1)).intValue();
			int itemId=new Integer(timeS.substring(1, 8)).intValue();
			item.setContent("Hello,world2"+timeS);
			
			item.setSubject("Hello, world"+timeS);
			TEItemMeta meta = new TEItemMeta();
			meta.setDateOfPost(System.currentTimeMillis());
			//System.out.println(i+":"+meta.getDateOfPost());
		//	writeToFile(meta.getDateOfPost(),i,fileName);
			meta.setFirstExtractionDate(System.currentTimeMillis());
			meta.setForumID("abc"+timeS);
			meta.setForumName("abc"+timeS);
			meta.setForumUrl("http://www.google.com"+timeS);
			meta.setItem(item);
			meta.setItemID(itemId);
			meta.setItemType("BBS"+timeS);
			meta.setItemUrl("http://www.seeisee.com"+timeS);
			meta.setKeyword("keyword"+timeS);
			meta.setKeywordGroup("kg"+timeS);
			meta.setLatestExtractionDate(System.currentTimeMillis());
			meta.setPoster("poste2r"+timeS);
			meta.setPosterID("poster"+timeS);
			meta.setSimpleDateOfPost("2007-1-1");
			meta.setSiteID(itemId);
			meta.setSiteName("cic"+timeS);
			meta.setSource("cic"+timeS);
			meta.setSubject("dd"+timeS);
			meta.setThreadID(itemId);
			meta.setTopicPost(true);
			
			item.setMeta(meta);
			writeToFile(new Long(item.getMeta().getDateOfPost()).toString(),i,fileName);
			return item;
		}
	 TEItem getSampleItem(){
		TEItem item = new TEItem();
		item.setContent("Hello,world2");
		item.setSubject("Hello, world");
		TEItemMeta meta = new TEItemMeta();
		meta.setDateOfPost(System.currentTimeMillis());
		
	//	writeToFile(meta.getDateOfPost(),0,"/home/ellen/testdata2/write2.txt");
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
	 
	 public void writeToFile(String text,int i,String fileName)
	 {
		 try {
			RandomAccessFile myFileStream=new RandomAccessFile(fileName,"rw");
			myFileStream.seek(myFileStream.length());
			StringBuffer tempS=new StringBuffer(i+":"+text);
			int tempSL=tempS.length();
			if(tempSL<100)
			{
				for(int j=0;j<100-tempSL;j++)
				{
					tempS.append("*");
				}
			}
			
			myFileStream.writeBytes(tempS+"\n");
			myFileStream.close();
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		 
	 }
	 
	 public void writeItemToFile(TEItem item, int i, String fileName)
	 {
		 
	 }
	 
	 public boolean isEqual(IDFEngine engine,String fileName,int currentPos){
		 try {
			 
			RandomAccessFile myFileStream=new RandomAccessFile(fileName,"rw");
			myFileStream.seek(0);
		
			while(myFileStream.getFilePointer()!=myFileStream.length())
			{
				
				String temp=myFileStream.readLine();
				int index=temp.indexOf(":");
				temp=temp.substring((index+1),temp.indexOf("*")).trim();
				
				String tempE=new Long(engine.getItem(currentPos++).getMeta().getDateOfPost()).toString();
		//		log.info((currentPos-1)+":"+tempE+"=="+temp);
			//	String tempE=new Long(engine.getItem(itemCount).getMeta().getThreadID()).toString();
				if(!tempE.equals(temp))
				{
					log.info((currentPos-1)+":"+tempE+"!="+temp);
					return false;
					
				}
			    
			}
			
			
			myFileStream.close();
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}catch(IDFEngineException e)
		{
			e.printStackTrace();
		}
		return true;
		 
	 }
	 public boolean isEqualAdd2(IDFEngine engine,String fileName,int extend){
		 long startT=System.currentTimeMillis();
		 boolean result=true;
		 int itemIndex=0;
		 int rowIndex=0;
		 int i=0;
		
		 
		 RandomAccessFile myFileStream;
		try {
			
			myFileStream = new RandomAccessFile(fileName,"rw");
			myFileStream.seek(0);
			while(i<extend/2&&(myFileStream.getFilePointer()!=myFileStream.length())){
				++i;
				++itemIndex;
				assertTrue(isEqualT(itemIndex,myFileStream));
				
				 
				 rowIndex++;
				 
				
				 if(myFileStream.getFilePointer()!=myFileStream.length()){
					 double tempSub=(extend/2+1)*((rowIndex-(extend*1.5+1))/(extend+1));
				 if((rowIndex-engine.getItemCount())==tempSub)
				 {
					 while(myFileStream.getFilePointer()!=myFileStream.length()){
						 ++itemIndex;
							assertTrue(isEqualT(itemIndex,myFileStream));
					 }
					 
				 }
				 else{
				 if(i==extend/2)
				 {
					 myFileStream.skipBytes((extend/2+1)*101);
					 i=0;
				 }
				 }
				 }
				 
			 }
			myFileStream.close();
			long processT=System.currentTimeMillis()-startT;
			log.info("add2 total process time="+format(processT));
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		}
		return result;
			
	 }
	 public boolean isEqualT(int itemIndex,  RandomAccessFile myFileStream){
		 try {
			 
			 
			String tempItem=new Long(engine.getItem(itemIndex).getMeta().getDateOfPost()).toString();
			
			String tempF=myFileStream.readLine();
			 tempF=tempF.substring(tempF.indexOf(":")+1, tempF.indexOf("*"));
			
			 assertTrue(tempItem.equals(tempF));
			 if(!tempItem.equals(tempF))
				 return false;
			
		} catch (IDFEngineException e) {
			
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		}
		return true;
		 
	 }
	 public boolean isEqualAdd(IDFEngine engine,String fileName,int extend){
		 long startT=System.currentTimeMillis();
		 int iniC=1;
		 int itemCount=0;
		 int rowCount=0;
		 int tempII=0;
		 boolean start=false;
		 boolean end=false;
		try {
			RandomAccessFile myFileStream = new RandomAccessFile(fileName,"rw");
			myFileStream.seek(0);
			while(myFileStream.getFilePointer()!=myFileStream.length()){
				rowCount++;
				
				String tempS=myFileStream.readLine();
				int index=tempS.indexOf(":");
				tempS=tempS.substring((index+1),tempS.indexOf("*")).trim();
				
				if(iniC%2==1)
				{
					itemCount++;
					String tempE=new Long(engine.getItem(itemCount).getMeta().getDateOfPost()).toString();
			//		log.info(itemCount+"="+tempE+"   "+" ac: "+rowCount+"="+tempS);
					
				//	String tempE=new Long(engine.getItem(itemCount).getMeta().getThreadID()).toString();
					if(!tempE.equals(tempS))
					{
						return false;
						
					}
					start=false;
				}
				else
				{
					tempII=(iniC/2-1)*(extend/2+1)+engine.getItemCount()-extend/2;
					if(((rowCount==tempII)&&(start==true))||(end==true))
					{
						//log.info("----tempII"+tempII+"iniC="+iniC);
						itemCount++;
						String tempE=new Long(engine.getItem(itemCount).getMeta().getDateOfPost()).toString();
						
				//		log.info(itemCount+"="+tempE+"   "+" ac: "+rowCount+"="+tempS);
						
					//	String tempE=new Long(engine.getItem(itemCount).getMeta().getThreadID()).toString();
						if(!tempE.equals(tempS))
						{
							//log.info("error");
							return false;
							
						}
						end=true;
						
					}
					else
					{
						start=false;
					}
					
					
				}
				
				int temp;
				if(iniC%2==1)
				{
					temp=(extend/2)*iniC+(iniC-1)/2;
					if(rowCount==temp){
						iniC++;
						start=true;
					//	log.info("*** rowCount="+rowCount+"iniC="+iniC+" isStatr="+start);
					}
				}
				else
				{
					temp=((extend/2)+1)*iniC-(iniC/2);
					if(rowCount==temp)
						{iniC++;
						start=true;
					//log.info("*** rowCount="+rowCount+"iniC="+iniC+" isStatr="+start);
						}
				}
				
				
			}
			if((itemCount)<engine.getItemCount())
			{
				/*log.info("itemCount="+itemCount+" amount="+engine.getItemCount());
				log.info("iniC="+iniC+" tempII"+tempII+" rowCount="+rowCount);*/
				return false;
				
			}
			
			myFileStream.close();
			
			long processT=System.currentTimeMillis()-startT;
			log.info("add process time="+format(processT));
			
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch(IDFEngineException e)
		{
			e.printStackTrace();
		}
		return true;
			
		 
	 }
	 
	 public boolean isEquals(ArrayList<TEItem> item_list,RandomAccessFile myFileStream)
		{
			TEItem item=null;
			String temp;
			for(int i=0;i<item_list.size();i++){
				item=item_list.get(i);
				
				try {
					temp = myFileStream.readLine();
					int index=temp.indexOf(":");
					temp=temp.substring((index+1),temp.indexOf("*")).trim();
					if(!temp.equals(new Long(item.getMeta().getDateOfPost()).toString())){
						log.info(temp+" ------  "+new Long(item.getMeta().getDateOfPost()).toString());
						return false;
					}
					
				} catch (IOException e) {
					
					e.printStackTrace();
				}
				
				
				
			}
			return true;
		}
	 public void iniFile(String fileN){
			try{
			RandomAccessFile raf=new RandomAccessFile(fileN,"rw");
			raf.setLength(0);
			raf.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
		}
	 
	
	}
	

}
