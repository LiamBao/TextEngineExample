package test.cic.textengine.idf;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.io.*;
import org.apache.log4j.Logger;

import test.cic.textengine.idf.IDFEngineTest.SampleItems;

import com.cic.textengine.idf.IDFEngine;
import com.cic.textengine.idf.IDFEnginePool;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

import junit.framework.TestCase;
/**
 * Purpose: to test whether an idf file can be read and written concurrently.
 * Steps:
 *     1. write some items to an idf file
 *     2. start a read thread and a write thread concurrently, the read thread is used to read the items
 *        which are appended at step 1, the write thread continues to append items to the same idf file.
 * Check points:
 *     1. check whether the items are appended correctly
 *     2. check whether the items can be read from idf file correctly by IDFEngine.getItems()
 * The variables which should be set before startup the application are as following:
 *     1. String folder;//used to set the folder path which contains the idf file and the txt file
 *     2. int amount;//used to set the amount of items which are appended when the application startup
 *     3. int appendAmount;//used to set the amount of items which are appended at the write thread
 *     4. final static int CLEAR_LIST=2000;//used to set the length of the item_id_list which is passed to IDFEngine.getItems()
 * Expected results:
 * the console will display the following strings at the end:
 *         Read thread success
 *         write second time success
 *
 *         
 * @author ellen
 *
 */
public class IDFEngineThreadTest  {
	Logger log = Logger.getLogger(IDFEngineThreadTest.class);
	File file=null;
	String appendFileName=null;
	String fileName=null;
	String folder=null;
	IDFEngine engine = null;
	int amount;
	int appendAmount;
	String idfName=null;
	IDFEngineTest idfTest=new IDFEngineTest();
	int startPos;
	final static int CLEAR_LIST=2000;
	protected void setUp() throws Exception {
		//super.setUp();
		folder="/home/ellen/testdata2/";
		idfName=folder+"append5.idf";
		file=new File(idfName);
		fileName=folder+"write6.txt";
		appendFileName=folder+"write5.txt";
		IDFEngineTest idfTest=new IDFEngineTest();
		IDFEngineTest.SampleItems si=idfTest.new SampleItems();
		si.iniFile(fileName);
		si.iniFile(appendFileName);
		si.iniFile(idfName);
		engine = IDFEnginePool.getInstance().getIDFEngineInstance(file);
		startPos=engine.getItemCount();
		amount=5000;
		appendAmount=5000;
	}

	
	
	public static void main(String[]args){
		IDFEngineThreadTest itt=new IDFEngineThreadTest();
		try {
			itt.setUp();
		
		itt.appendItems(itt.amount,itt.fileName);
		IDFEngineThreadTest.WriteFileThread writeThread=itt.new WriteFileThread();
		IDFEngineThreadTest.ReadFileThread readThread=itt.new ReadFileThread();
		
		writeThread.start();
		itt.log.info("write thread start");
		readThread.start();
		itt.log.info("read thread start");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	class WriteFileThread extends Thread{
		public void run()
		{
			appendItems(appendAmount,appendFileName);
			IDFEngineTest.SampleItems si=idfTest.new SampleItems();
			boolean tB=si.isEqual(engine, appendFileName, amount+1);
			if(tB==false)
			{
				log.info("write second time error");
				return;
			}
			else
			{
				log.info("write second time success");
			}
		}
	}
	class ReadFileThread extends Thread{
		public void run()
		{
			ArrayList<Integer>item_id_list=new ArrayList<Integer>();
			int loop=0;
			boolean temp=false;
			for(int i=startPos+1;i<=amount;i++)
			{
				item_id_list.add(i);
				if(item_id_list.size()>CLEAR_LIST){
					loop+=item_id_list.size();
					temp=clearList(item_id_list,true,fileName);
					
				}
				
			}
			
			if(item_id_list.size()>0)
				{
				loop+=item_id_list.size();
				temp=clearList(item_id_list,true,fileName);
				
				}
			if(temp==true){
				log.info("Read thread success");
			}
			else
				log.info("Read thread failed");
		/*	
			try {
				log.info("amount="+amount+"/"+engine.getItem(amount).getMeta().getDateOfPost());
				log.info("loop"+loop);
			} catch (IDFEngineException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
			
			
		}
	}
	public void appendItems(int amount2,String fileN){
		
		IDFEngineTest.SampleItems si=idfTest.new SampleItems();
		si.appendSampleItems(engine, amount2, fileN);
	}
	public boolean clearList(ArrayList<Integer>item_id_list,boolean sorted,String fileN){
		ArrayList<TEItem>item_list=new ArrayList<TEItem>();
		boolean tempB=true;
		try {
			item_list=engine.getItems(item_id_list, sorted);
		//	ArrayList<String>arrayFromItem=new ArrayList<String>();
			ArrayList<String>arrayFromFile=getItemFromFile(item_id_list,fileN);
			for(int i=0;i<item_list.size();i++)
			{
				String itemS=new Long(item_list.get(i).getMeta().getDateOfPost()).toString();
				//arrayFromItem.add((new Long(item_list.get(i).getMeta().getDayOfPost())).toString());
				tempB=(itemS.equals(arrayFromFile.get(i)));
				
				if(tempB==false)
				{
					log.error("Read thread error cause the first write error, the error items is as following:");
					log.error("itemID"+(i+1)+": error data in idf file="+itemS+" correct data="+arrayFromFile.get(i));
					return false;
					
				}
				/*else
					log.info((i+1)+"="+itemS+"/"+arrayFromFile.get(i));*/
				
			}
			
			
		
			
			
		} catch (IDFEngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		item_id_list.clear();
		return true;
	}
	public ArrayList<String>getItemFromFile(ArrayList<Integer>item_id_list,String fileN){
		ArrayList<String>array=new ArrayList<String>();
		try {
			RandomAccessFile raf=new RandomAccessFile(fileN,"rw");
			
			for(int i=0;i<item_id_list.size();i++){
				raf.seek(0);
				raf.skipBytes((item_id_list.get(i).intValue()-1)*101);
				String tempS=raf.readLine();
			   tempS=tempS.substring(tempS.indexOf(":")+1, tempS.indexOf("*"));
			   array.add(tempS);
				   
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return array;
		
	}
	
	
}
