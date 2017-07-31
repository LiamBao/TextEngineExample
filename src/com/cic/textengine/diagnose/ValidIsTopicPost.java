package com.cic.textengine.diagnose;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.type.PartitionKey;

public class ValidIsTopicPost {

	private static String repPath = "/home/CICDATA/te_opr/TERepo";
	private static String bakPath = "/home/CICDATA/te_opr/TERepo_bak";
	/**
	 * @param args
	 * @throws RepositoryEngineException 
	 * @throws DecoderException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws RepositoryEngineException, DecoderException, IOException {
		if(args.length < 2)
		{
			System.out.println("Two parameters: repoPath bakPath");
			return;
		}
		repPath = args[0].trim();
		bakPath = args[1].trim();
		RepositoryEngine repoEng = RepositoryFactory.getNewRepositoryEngineInstance(repPath);
		RepositoryEngine backEng = RepositoryFactory.getNewRepositoryEngineInstance(bakPath);
		ArrayList<String> parkeyList = getPartitionKey(bakPath);
		ArrayList<String> invalidKey = new ArrayList<String>();
		for(String parkey: parkeyList) {
			PartitionKey key = PartitionKey.decodeStringKey(parkey);
			int year = key.getYear();
			int month = key.getMonth();
			String siteid = key.getSiteID();
			String forumid = key.getForumID();
			PartitionEnumerator enu_bak = backEng.getPartitionEnumerator(year, month, siteid, forumid, 0, true);
			long itemCountBak = 0;
			while(enu_bak.next()) {
				itemCountBak ++;
			}
			enu_bak.close();
			PartitionEnumerator enu = repoEng.getPartitionEnumerator(year, month, siteid, forumid, 0, true);
			long itemCount = 0; 
			while(enu.next()) {
				itemCount ++;
			}
			enu.close();
			if(itemCount < itemCountBak) {
				invalidKey.add(parkey);
				System.out.println("Invalid parkey: "+parkey);
			}
		}
		for(String parkey: invalidKey) {
			System.out.println(parkey);
		}
	}
	
	public static ArrayList<String> getPartitionKey(String path) {
		ArrayList<String> parkeyList = new ArrayList<String>();
		File dir = new File(path);
		if(dir.isDirectory()) {
			String[] fileList = dir.list();
			for(String fileName : fileList) {
				String[] parkey_org = fileName.trim().split("_");
				String siteid = parkey_org[1].trim();
				String forumid = parkey_org[2].trim();
//				String[] parkey_file = parkey_org[0].trim().split("_");
//				String siteid = parkey_file[1].trim();
//				String forumid = parkey_file[2].trim();
				String parkey = siteid+"_2008_"+forumid+"_11";
				if(!parkeyList.contains(parkey))
					parkeyList.add(parkey);
			}
		}
		return parkeyList;
	}

}
