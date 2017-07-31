package test.cic.textengine.idf;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.log4j.Logger;



import com.cic.textengine.repository.datanode.repository.PartitionSearcher;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.type.TEItem;
/**
 * Purpose: to test whether a partition can be read and written concurrently.
 * Steps:
 *          1. used PartitionWriter.writeItem() to write Item to local partition
 *          2. start a write thread which use used PartitionWriter.writeItem() 
 *             to write Item to local partition and a search thread which use 
 *             PartitionSearcher.queryItems() to search the items which are 
 *             added by the step 1.
 * The variables which should be set before startup the application are as following:
 *          1.String engineFolder;// set the partition folder
 *          2.BigInteger amount;  // set the amount of items which will be added to partition
 *          3.int year;int month;String siteid;String forumid;//set the partition key
 *          
 * Expected results:
 *  the console will display the following strings at the end:     
 *     Read successfully!
 *     write Second time successfully
 * @author ellen
 *
 */
public class PartitionThreadTest {

	Logger log = Logger.getLogger(PartitionThreadTest.class);
	RepositoryEngine engine=null;
	PartitionWriter writer=null;
	IDFEngineTest idfTest=null;
	IDFEngineTest.SampleItems si=null;
	String fileName=null;
	long startT;
	long processT;
	
	BigInteger amount;
	String engineFolder;
	String fileName2=null;
	 PartitionSearcher ps;
	 PartitionTest pt=null;
	 long startPos;
	 PartitionTest.PartitionSample p_sample=null;
	 String forumid=null;
	 String siteid=null;
	 int year;
	 int month;
	 final static int CLEAR_LIST=2000;
	public void setUp() throws Exception {
		year=1993;
		month=2;
		siteid="google.com";
		forumid="ff_qq";
		engineFolder="/home/ellen/testdata2/";
	
		engine=RepositoryFactory.getNewRepositoryEngineInstance(engineFolder);
		writer=engine.getPartitionWriter(year,month, siteid, forumid, 1l);
		idfTest=new IDFEngineTest();
		si=idfTest.new SampleItems();
		fileName=engineFolder+"writePartition.txt";
		fileName2=engineFolder+"writePartition2.txt";
		si.iniFile(fileName2);
		si.iniFile(fileName);
		amount=new BigInteger("12000");
		
		ps = engine.getPartitionSearcher(year,month, siteid, forumid);
		pt=new PartitionTest();
		p_sample=pt.new PartitionSample();
		writeItems(writer,fileName,amount);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		//write items
		PartitionThreadTest ptt=new PartitionThreadTest();
		try{
		ptt.setUp();
		
		PartitionThreadTest.WriteThread wt=ptt.new WriteThread();
		PartitionThreadTest.SearchThread pss=ptt.new SearchThread();
	
		wt.start();
		System.out.println("write thread start");
		
		pss.start();
		System.out.println("search thread start");
		
		
		}
		catch(RepositoryEngineException e){
			e.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}
		

	}
	public void writeItems(PartitionWriter writer,String fileN,BigInteger amount)
	{
	     BigInteger i=new BigInteger("0");
	     BigInteger addI=new BigInteger("1");
	     try{
	      while(i.subtract(amount).intValue()<0)
	      {
	    	  i=i.add(addI);
	    	  TEItem item=si.getSampleItem2(i.intValue(),fileN);
	    	
	    	  writer.writeItem(item);
	    	  //log.info("write item"+i+"="+item.getMeta().getDateOfPost());
	      }
	      writer.flush();
	      writer.close();
	      
	      }catch(RepositoryEngineException e){
				e.printStackTrace();
			}
	}
	class WriteThread extends Thread{
		public void run(){
			writeItems(writer,fileName2,amount);
			boolean tempB=isEqual(amount.longValue()+1l,amount,fileName2);
			if(tempB==false)
			{log.info("Write Second time Error!");
			return;
			}
		else
			log.info("write Second time successfully!");
		}
	}
  class SearchThread extends Thread{
	  public void run(){
			boolean tempB=isEqual(1l,amount,fileName);
			if(tempB==false)
				{log.info("Read Error!");
				return;
				}
			else
			log.info("Read successfully!");
		     
			
		  
	  }
  }
     public boolean isEqual(long startP,BigInteger amount,String fileN)
     {
    	 ArrayList<Long> item_id_list=new ArrayList<Long>();
		
			RandomAccessFile raf;
			try{
				raf = new RandomAccessFile(fileN,"rw");
            long i=0l;
		    
		     while(i<amount.longValue())
		      {
		    	 
		    	 item_id_list.add(i+startP);
		    	 if(item_id_list.size()>CLEAR_LIST){
		    		
		    	isEqualF(startP,item_id_list,raf,true);
		    	
		    	 }
		    	  i=i+1l;
		    	  
		      }
		     if(item_id_list.size()>0)
		     {
		    	
			    	isEqualF(startP,item_id_list,raf,true);
			    	
		     }
		     
		
			}catch(IOException e){
				e.printStackTrace();
			}
				
					/*else
						log.info((k+1)+tempF+"/"+tempI);*/
				
				
		
			return true;
     }
     
     public boolean isEqualF(long startP,ArrayList<Long> item_id_list,RandomAccessFile raf,boolean sorted){
    	ArrayList<TEItem> item_list;
		try {
			item_list = ps.queryItems(item_id_list, sorted);
		
    	 if(startP!=1l)
			{
				for(int j=0;j<item_id_list.size();j++)
				{
					long temp=item_id_list.get(j);
					item_id_list.set(j, temp-startP+1l);
				}
			}
			ArrayList<Long>array=p_sample.getFileString(raf, item_id_list);
			for(int k=0;k<item_id_list.size();k++)
			{
				long tempF=array.get(k).longValue();
				long tempI=item_list.get(k).getMeta().getDateOfPost();
				if(tempF!=tempI)
				{
					log.info("error write=>"+(k+1)+tempF+"/"+tempI);
					return false;
				}}
    	 
    	 item_id_list.clear();
			} catch (RepositoryEngineException e) {
			
				e.printStackTrace();
			}
    	 return true;
    	 
    	 
     }
			
     
	
}
