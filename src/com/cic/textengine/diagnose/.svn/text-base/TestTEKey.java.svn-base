package com.cic.textengine.diagnose;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.exception.TEClientException;
import com.cic.textengine.type.TEItem;

public class TestTEKey {

	/**
	 * @param args
	 * @throws IOException 
	 */
	
	public static ArrayList<String> loadKeys(String file) throws IOException{
		ArrayList<String> results = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		while((line=br.readLine()) != null){
			results.add(line.trim());
		}
		br.close();
		return results;
	}
	
	public static void printErrorKeys(ArrayList<String> keys){
		TEClient client = new TEClient("192.168.2.2", 6869);
		int count = 0;
		for(String key: keys){
			try {
				TEItem item = client.getItem(key);
			} catch (TEClientException e) {
				System.out.println(key);
			}
			count ++;
			if (count % 5000 == 0)
				System.out.println(count);
		}
		client.close();
	}
	public static void main(String[] args) throws IOException {
		if(args.length<1){
			System.out.println("Usage: keyFile");
			return;
		}
		
		ArrayList<String> keys = loadKeys(args[0].trim());
		printErrorKeys(keys);

	}

}
