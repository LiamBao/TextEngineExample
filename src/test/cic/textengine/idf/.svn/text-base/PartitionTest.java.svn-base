package test.cic.textengine.idf;

import org.apache.log4j.Logger;

import junit.framework.TestCase;

import com.cic.textengine.repository.datanode.repository.*;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.*;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.util.ArrayList;
import java.lang.Math;
import java.util.HashSet;

import java.util.Collections;

/**
 * 
 * @author ellen
 *
 * Purpose: to test the following methods in package com.cic.textengine.repository.datanode.repository
 *              1.PartitionWriter.writeItem(TEItem item)
 *              2.PartitionWriter.flush()
 *              3.PartitionWriter.close() 
 *              4.PartitionSearcher.queryItem(long ItemID)
 *              5.PartitionSearcher.queryItems(ArrayList<Long> item_id_list,boolean sorted)
 *              6.PartitionEnumerator.next()
 *              7.PartitionEnumerator.getItem()
 *              8.RepositoryEngine.clean()
 *              9.RepositoryEngine.deleteItems(int year, int month, String siteid, String forumid, ArrayList<Long> itemids, boolean sorted)
 * General parameters: 
 *              @param String engineFolder the partition folder 
 * TestCases:
 *                   Test Case         |             Usage             |          Parameters
 *            --------------------------------------|------------------------------------------|-----------------------------------------------------------------
 *            testPartitionWrite()     |write items to a partition     |int maxNum //the amount of items to add
 *            --------------------------------------|------------------------------------------|----------------------------------------------------------------
 *            testPartitionSearch()    |query items from startPos to          |int maxNum //the amount of item count in the idf
 *                                     |maximum in idf                 |
 *                                     |(PartitionSearcher.queryItem())|
 *           ---------------------------------------|------------------------------------------|------------------------------------------------------------------
 *           testPartitionSearchList() |query items from startPos to   |int maxNum //the amount of item count in the idf
 *                                     |maximum  in idf                |int listMax  //the size of item_id_list to query
 *                                     |(PartitionSearcher.queryItems()|
 *           ---------------------------------------|------------------------------------------|-------------------------------------------------------------------
 *           testSearchRandomList()    |query items at random          |int firstItemCount//the amount that in the first idf
 *                                     |(PartitionSearcher.queryItems()|int secondeItemCount//the amount that in other idfs except 1st
 *          ----------------------------------------|------------------------------------------|------------------------------------------------------------------------------------
 *          testPartitionEnumerator()  |query items from startPos to   |
                                       |the end                        | 
 *                                     |PartitionEnumerator.next()/    |
 *                                     |PartitionEnumerator.getItem()  |
 *         -----------------------------------------|------------------------------------------|------------------------------------------------------------------------------------
 *           testRepositoryClean()     |test RepositoryEngine.clean()  |
 *         -----------------------------------------|------------------------------------------|-------------------------------------------------------------------------------------
 *         testRepositoryDeleteItems() |test RepositoryEngine.         |if boolean sorted==true, then delete Items from long startDelPos to long deletePos 
 *                                     |deleteItems()                  |if boolean sorted==false, then delete Items at random.
 *                                        
 *                                     
 *
 */
public class PartitionTest extends TestCase {
	Logger log = Logger.getLogger(PartitionTest.class);
	RepositoryEngine engine=null;
	PartitionWriter writer=null;
	IDFEngineTest idfTest=null;
	IDFEngineTest.SampleItems si=null;
	String fileName=null;
	long startT;
	long processT;
	int maxNum=0;
	String engineFolder=null;
	int listMax=0;
	int firstItemCount;
	int secondItemCount;
	PartitionEnumerator pEnumerator=null;
	int year;
	int month;
	String siteid;
	String forumid;
	long startPos;
	PartitionSearcher ps;
	long startDelPos;
	long deletePos;
	boolean sorted;
	
	public void setUp() throws Exception {
		engineFolder="/home/ellen/testdata2/";
		engine=RepositoryFactory.getNewRepositoryEngineInstance(engineFolder);
		year=1993;
		month=2;
		siteid="www.google.com";
		forumid="c";
		startPos=2L;
		startDelPos=2L;
		deletePos=52L;
		//10000000L
		writer=engine.getPartitionWriter(year, month, siteid, forumid, startPos);
		idfTest=new IDFEngineTest();
		si=idfTest.new SampleItems();
		fileName=engineFolder+"writePartition.txt";
	    
		listMax=2000;
		maxNum=1000;//must larger than 1048576 if run testSearchRandomList()
		firstItemCount=3000;
		secondItemCount=1000;
		ps = engine.getPartitionSearcher(year, month, siteid, forumid);
		sorted=false;
		
	}
	
	public void AtestPartitionWrite(){
		
		try {
			startT=System.currentTimeMillis();
			log.info("start at "+si.format(startT));
			si.iniFile(fileName);
			for(int i=1;i<=maxNum;i++)
			{	
				TEItem item=si.getSampleItem2(i, fileName);
				
			writer.writeItem(item);
			log.info(i+"="+item.getMeta().getDateOfPost());
			}
			writer.flush();
			writer.close();
			processT=System.currentTimeMillis()-startT;
			log.info("total process time = "+si.format(processT));
			
			
		//	log.info(ps.queryItem(1l).getMeta().getDateOfPost());
		   log.info(ps.queryItem(maxNum).getMeta().getDateOfPost());
		
		} catch (RepositoryEngineException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public void AtestRepositoryClean(){
		
	assertTrue(engine.clean());
	}
	
	public void testRepositoryDeleteItems(){
		ArrayList<Long>itemids=new ArrayList<Long>();
		HashSet<Long>itemidsH=new HashSet<Long>();
		
		
		
			while(startDelPos<=deletePos){
			 if(sorted)	
			 	itemids.add(startDelPos);
				// itemidsH.add(i);
			 else
			 {
				 double temp=0;
				 if(startDelPos<((deletePos+startDelPos)/2))
				 temp=deletePos*Math.random();
				 else
					 temp=(maxNum-1048576)*Math.random()+1048576;
				if((long)temp==0)temp=1;
				 itemidsH.add((long)temp);
				//itemids.add((long)temp);
			 }
			// log.info("new item id"+i+"="+itemids.get((int)i));
			 startDelPos++;
				
			}
         
		try {
			
			if(!sorted)
			itemids.addAll(itemidsH);
			for(int j=0;j<itemids.size();j++)
			{
				log.info((j+1)+"="+itemids.get(j));
			}
			engine.deleteItems(year, month, siteid, forumid, itemids, sorted);
			ArrayList<TEItem>item_list=ps.queryItems(itemids, sorted);
			//log.info(item_list.size());
		//	log.info(item_list.get(0).getMeta().getDateOfPost());
		//TEItem item=ps.queryItem(2l);
	
	//		log.info("&****"+item.getMeta().getDateOfPost());
			for(int ii=0;ii<itemids.size();ii++)
			{
				assertNull(ps.queryItem(itemids.get(ii)));
			}
			//assertNull(item);
		assertTrue(item_list.size()==0);
			
		} catch (RepositoryEngineException e) {
			
			e.printStackTrace();
		}
	}
	
	
	public void AtestPartitionSearch(){
		
		
		try {
		
		
			
			RandomAccessFile myFileStream;
			
				myFileStream = new RandomAccessFile(fileName,"rw");
				myFileStream.seek(0);
				Long i=startPos;
				maxNum+=startPos;
				String temp=null;
				while(i-maxNum<0)
				{
					String dateOfPost=new Long(ps.queryItem(i).getMeta().getDateOfPost()).toString();
					temp = myFileStream.readLine();
					int index=temp.indexOf(":");
					temp=temp.substring(index+1,temp.indexOf("*")).trim();
					log.info(i+"="+dateOfPost+" temp="+temp);
					assertTrue(dateOfPost.equals(temp));
					i=i+1l;
				}
				myFileStream.close();
				
				
			} catch (Exception e) {
				
				e.printStackTrace();
			} 
			
			
		}
	
		   
	
	
	public void AtestPartitionSearchList(){
		
		try {
			
			PartitionSample p_sample=new PartitionSample();
			
			ArrayList<Long> item_id_list=new ArrayList<Long>();
			ArrayList<TEItem>item_list=new ArrayList<TEItem>();
			BigInteger i=new BigInteger(new Long(startPos).toString());
			BigInteger max=(new BigInteger(new Integer(maxNum).toString())).add(i);
			BigInteger temp=new BigInteger("0");
			BigInteger addI=new BigInteger("1");
			
			RandomAccessFile myFileStream = new RandomAccessFile(fileName,"rw");
			myFileStream.seek(0);
			log.info("Start....");
			startT=System.currentTimeMillis();
			while(!i.equals(max))
			{
				item_id_list.add(i.longValue());
				
				temp=temp.add(addI);
				
				
				if(temp.equals(listMax)){
					
					item_list=ps.queryItems(item_id_list, false);
				
					assertTrue(p_sample.isEqual(item_list, myFileStream));
					
					item_id_list.clear();
					temp=new BigInteger("0");
				}
			
				i=i.add(addI);
				
				
			}
		    if(item_id_list.size()>0){
		    	item_list=ps.queryItems(item_id_list, false);
			
				assertTrue(p_sample.isEqual(item_list, myFileStream));
				item_id_list.clear();
		    	
		    }
		    myFileStream.close();
			log.info("The total process time="+si.format(System.currentTimeMillis()-startT));
			
			
		} catch (RepositoryEngineException e) {
			
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public void AtestSearchRandomList(){
		PartitionSearcher ps;
		PartitionSample p_sample=new PartitionSample();
		
			try {
				RandomAccessFile myFileStream = new RandomAccessFile(fileName,"rw");
				ps = engine.getPartitionSearcher(1993, 2, "www.google.com", "c");
				ArrayList<Long> item_id_list=new ArrayList<Long>();
				ArrayList<TEItem>item_list=new ArrayList<TEItem>();
			
				double randL;
				for(int i=0;i<firstItemCount;i++)
				{
					randL=maxNum*Math.random()%1048576;
					item_id_list.add((long)randL);
					
				}
				for(int i=0;i<secondItemCount;i++)
				{
					randL=maxNum*Math.random();
					if(randL<=1048576)
					{
						i--;
					}
					else
					item_id_list.add((long)randL);
				}
				/*for(int i=0;i<item_id_list.size();i++)
				{
					log.info((i+1)+"="+item_id_list.get(i));
				}*/
				startT=System.currentTimeMillis();
				/*Collections.sort(item_id_list);
				log.info("after sort");
				for(int i=0;i<item_id_list.size();i++)
				{
					log.info((i+1)+"="+item_id_list.get(i));
				}
				log.info("**********");*/
				
				item_list=ps.queryItems(item_id_list, false);
				log.info("query finish");
				ArrayList<Long>array=p_sample.getFileString(myFileStream, item_id_list);
				log.info("load all string from txt file");
				
			/*	Collections.sort(item_id_list);
				log.info("finish sort...");*/
				for(int i=0;i<item_id_list.size();i++){
					//log.info("----------");
					Long tempS=new Long(item_list.get(i).getMeta().getDateOfPost());
					Long tempS2=array.get(i);
				
					//log.info(item_id_list.get(i)+"= "+tempS+" sample="+tempS2);
				assertTrue(tempS.equals(tempS2));
					
				}
				myFileStream.close();
				log.info("The total process time= "+si.format(System.currentTimeMillis()-startT));
				
				
			} catch (RepositoryEngineException e) {
				
				e.printStackTrace();
			} catch(IOException e)
			{
				e.printStackTrace();
			}
	}
	
	
	
	public void AtestPartitionEnumerator(){
		try {
			TEItem item=null;
			ArrayList<TEItem>item_list=new ArrayList<TEItem>();
		//	fileName=("/home/ellen/testdata2/write9/wwww1.txt");
			
			pEnumerator=engine.getPartitionEnumerator(year, month, siteid, forumid);
			
			RandomAccessFile myFileStream = new RandomAccessFile(fileName,"rw");
			myFileStream.seek(0);
			
			PartitionSample p_sample=new PartitionSample();
			int i=0;
			long temp=0l;
			int itemSize=0;
			
			while(pEnumerator.next())
			{
				i++;
				temp++;
				if(temp>=startPos)
				{
					item=pEnumerator.getItem();
				   item_list.add(item);
				}
				if(i>listMax){
					
					assertTrue(p_sample.isEqual(item_list, myFileStream));
					
					itemSize+=item_list.size();
					item_list.clear();
					i=0;
				}
				
				
			}
			if(item_list.size()>0){
			assertTrue(p_sample.isEqual(item_list, myFileStream));
				itemSize+=item_list.size();
			}
			log.info(itemSize+"="+myFileStream.length()/101);
			assertTrue(itemSize==myFileStream.length()/101);
			pEnumerator.close();
			myFileStream.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (RepositoryEngineException e) {
			
			e.printStackTrace();
		}	
		
	}
	
	
	class PartitionSample{
		public boolean isEqual(ArrayList<TEItem> item_list,RandomAccessFile myFileStream)
		{
			TEItem item=null;
			String temp;
			for(int i=0;i<item_list.size();i++){
				item=item_list.get(i);
				
				try {
					temp = myFileStream.readLine();
					int index=temp.indexOf(":");
					temp=temp.substring(index+1,temp.indexOf("*")).trim();
					if(!temp.equals(new Long(item.getMeta().getDateOfPost()).toString())){
						
						log.info("temp="+temp+" item="+item.getMeta().getDateOfPost());
						return false;
					}
					
				} catch (IOException e) {
					
					e.printStackTrace();
				}
				
				
				
			}
			return true;
		}
		
		public ArrayList<Long> getFileString(RandomAccessFile myFileStream, ArrayList<Long> pos_list){
			
			
			ArrayList<Long>array=new ArrayList<Long>();
			Collections.sort(pos_list);
			try{
			for(int i=0;i<pos_list.size();i++){
				myFileStream.seek(0);
			   myFileStream.skipBytes((pos_list.get(i).intValue()-1)*101);
			   String tempS=myFileStream.readLine();
			   tempS=tempS.substring(tempS.indexOf(":")+1, tempS.indexOf("*"));
			   
			   array.add(new Long(tempS));
			//   log.info(pos_list.get(i)+"=="+array.get(i));
				
			}}catch(IOException e){
				e.printStackTrace();
			}
			
			
			return array;
		}
		
		
	}
	
	
	
	

}
