package com.cic.textengine.diagnose;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.repository.datanode.repository.PartitionEnumerator;
import com.cic.textengine.repository.datanode.repository.PartitionWriter;
import com.cic.textengine.repository.datanode.repository.RepositoryEngine;
import com.cic.textengine.repository.datanode.repository.RepositoryFactory;
import com.cic.textengine.repository.datanode.repository.exception.RepositoryEngineException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class FixInvalidParkey {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws RepositoryEngineException 
	 * @throws DecoderException 
	 */
	public static void main(String[] args) throws IOException, RepositoryEngineException, DecoderException {
		// TODO Auto-generated method stub
		if(args.length < 4) {
			System.out.println("4 parameters needed: repoPath bakPath parKeyFile itemKeyFile");
			return;
		}
		
		String repPath = args[0];
		String bakPath = args[1];
		String keyFile = args[2];
		String itemKeyFile = args[3];

		// read the item keys
		
		System.out.println("Reading the Item Keys...");
		
		ArrayList<String> itemkeyList = new ArrayList<String>();
		try {
			FileReader fr = new FileReader(itemKeyFile);
			BufferedReader br = new BufferedReader(fr);
			String itemkey = null;
			while((itemkey = br.readLine()) != null) {
				itemkeyList.add(itemkey);
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
				e.printStackTrace();
			}
		}
		
		System.out.println("Got all itemkeys.");
		
		FileReader fr = new FileReader(keyFile);
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		ArrayList<String> parkeyList = new ArrayList<String>();
		while((line=br.readLine()) != null) {
//			String[] words = line.split(":");
//			String parkey = words[1].trim();
			String parkey = line.trim();
			parkeyList.add(parkey);
		}
		RepositoryEngine repoEng = RepositoryFactory.getNewRepositoryEngineInstance(repPath);
		RepositoryEngine backEng = RepositoryFactory.getNewRepositoryEngineInstance(bakPath);
		for(String parkey: parkeyList) {
			System.out.println("Refix partition: "+parkey);
			PartitionKey key = PartitionKey.decodeStringKey(parkey);
			PartitionEnumerator enu = backEng.getPartitionEnumerator(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID(), 0, true);
			PartitionWriter pw = repoEng.getPartitionWriter(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID(), 1);
			ArrayList<Long> idlist = parMap.get(parkey);
			while(enu.next()) {
				TEItem item = enu.getItem();
				if(idlist.contains(item.getMeta().getItemID()))
					item.getMeta().setTopicPost(true);
				pw.writeItem(item);
			}
			pw.close();
			enu.close();
		}
		
		for(String parkey: parkeyList) {
			PartitionKey key = PartitionKey.decodeStringKey(parkey);
			PartitionEnumerator bakEnu = backEng.getPartitionEnumerator(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID(), 0, true);
			long bakCount = 0;
			while(bakEnu.next())
				bakCount ++;
			bakEnu.close();
			PartitionEnumerator repEnu = repoEng.getPartitionEnumerator(key.getYear(), key.getMonth(), key.getSiteID(), key.getForumID(), 0, true);
			long repCount = 0;
			while(repEnu.next()) {
				repCount ++;
			}
			repEnu.close();
			if(repCount < bakCount) {
				System.out.println("Still invalid: "+parkey);
			}
		}
		System.out.println("Job Done!");
	}

}
