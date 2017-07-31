package com.cic.textengine.diagnose;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class UpdateIsTopicPost {

	private static String repPath = "/home/CICDATA/te_opr/TERepo";
	private static String bakPath = "/home/CICDATA/te_opr/TERepo";
	private static String keyFile = "/home/CICDATA/te_opr/";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length < 3) {
			System.out.println("Three parameters needed: repPath bakPath keyFile");
			return;
		}
		repPath = args[0].trim();
		bakPath = args[1].trim();
		keyFile = args[2].trim();
		
		// read the item keys
		
		System.out.println("Reading the Item Keys...");
		
		ArrayList<String> itemkeyList = new ArrayList<String>();
		try {
			FileReader fr = new FileReader(keyFile);
			BufferedReader br = new BufferedReader(fr);
			String itemkey = null;
			while((itemkey = br.readLine()) != null) {
				itemkeyList.add(itemkey.trim());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// group the item by the partition
		HashMap<String, ArrayList<Long>> parMap = new HashMap<String, ArrayList<Long>>();
		for (String itemkey : itemkeyList) {
			try {
				ItemKey key = ItemKey.decodeKey(itemkey);
				String parKey = key.getPartitionKey();
				long id = key.getItemID();
				if (!parMap.keySet().contains(parKey))
					parMap.put(parKey, new ArrayList<Long>());
				parMap.get(parKey).add(id);
			} catch (DecoderException e) {
				// TODO Auto-generated catch block
				System.out.println(itemkey);
				e.printStackTrace();
			}
		}

		System.out.println(String.format(
				"Totally, there are %s items to fix in %s partitions.",
				itemkeyList.size(), parMap.keySet().size()));
		itemkeyList = null;
		
		// If the partition is in the repository, move the IDF file to the backup directory.
		try {
			ArrayList<String> parkeyList = new ArrayList<String>();
			for(String parkey : parMap.keySet()) {
				PartitionKey key = null;
				try {
					key = PartitionKey.decodeStringKey(parkey);
				} catch (DecoderException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				int year = key.getYear();
				int month = key.getMonth();
				String siteid = key.getSiteID();
				String forumid = key.getForumID();
				
				int i = 0;
				String idfName = buildIDFFileName(year, month, siteid, forumid, i);
				File dataFile = new File(repPath+File.separator+idfName);
				File indexFile = new File(dataFile.getAbsolutePath() + ".idx");
				while(dataFile.exists() && dataFile.isFile()
						&& indexFile.exists() && indexFile.isFile()) {
					// record the partition need to be fixed
					if(i==0)
						parkeyList.add(parkey);
					// move the data file and the index file to the backup directory
					File dataFile_bak = new File(bakPath+File.separator+dataFile.getName());
					File indexFile_bak = new File(bakPath+File.separator+indexFile.getName());
					moveSourceToDest(dataFile, dataFile_bak);
					moveSourceToDest(indexFile, indexFile_bak);
					// test if there are more IDF files
					i++;
					idfName = buildIDFFileName(year, month, siteid, forumid, i);
					dataFile = new File(repPath+File.separator+idfName);
					indexFile = new File(dataFile.getAbsolutePath() + ".idx");
				}
			}
			
			// recreate the IDF files
			
			System.out.println("Start recreating the IDF files...");
			
			RepositoryEngine repoEng = RepositoryFactory.getNewRepositoryEngineInstance(repPath);
			RepositoryEngine backEng = RepositoryFactory.getNewRepositoryEngineInstance(bakPath);
			for(String parkey: parkeyList) {
				PartitionKey key = PartitionKey.decodeStringKey(parkey);
				PartitionEnumerator enu = backEng.getPartitionEnumerator(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID());
				PartitionWriter writer = repoEng.getPartitionWriter(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID(), 1);
				ArrayList<Long> idlist = parMap.get(parkey);
				while(enu.next()) {
					TEItem item = enu.getItem();
					if(idlist.contains(item.getMeta().getItemID()))
						item.getMeta().setTopicPost(true);
					writer.writeItem(item);
				}
				writer.close();
				enu.close();
				System.out.println(String.format("Partition %s done!", parkey));
			}
			System.out.println("Job Done!");
			
		} catch (RepositoryEngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static String buildIDFFileName(int year, int month, String siteid, String forumid, int idx) throws UnsupportedEncodingException{
		/*
		 * String b64_siteid = new
		 * String(Base64.encodeBase64(siteid.trim().getBytes(),false)); String
		 * b64_forumid = new
		 * String(Base64.encodeBase64(forumid.trim().getBytes(),false));
		 */
		String hex_siteid = new String(Hex.encodeHex(siteid.trim().getBytes("utf-8")));
		String hex_forumid = new String(Hex
				.encodeHex(forumid.trim().getBytes("utf-8")));
		String idf_name = formatMonthKey(year, month) + "_" + hex_siteid + "_"
				+ hex_forumid + "_" + idx + ".idf";
		return idf_name;
	}
	private static String formatMonthKey(int year, int month){
		String result = Integer.toString(year);
		if (month >= 10){
			result += Integer.toString(month);
		}else{
			result += "0" + Integer.toString(month);
		}
		return result;
	}
	private static void moveSourceToDest(File src, File dest) throws IOException {
		// Create channel on the source
		FileChannel srcChannel = new FileInputStream(src).getChannel();

		// Create channel on the destination
		FileChannel dstChannel = new FileOutputStream(dest).getChannel();

		// Copy file contents from source to destination
		dstChannel.transferFrom(srcChannel, 0, srcChannel.size());

		// Close the channels
		srcChannel.close();
		dstChannel.close();

		// Remove the source file
		src.delete();
		
		System.out.println(String.format("File %s moved to %s", src, dest));
	}
}
