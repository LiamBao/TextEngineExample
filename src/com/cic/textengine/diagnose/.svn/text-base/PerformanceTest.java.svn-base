package com.cic.textengine.diagnose;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.TEItemEnumerator;
import com.cic.textengine.client.exception.TEClientException;
import com.cic.textengine.client.exception.TEItemEnumeratorException;
import com.cic.textengine.repository.type.ItemKey;

public class PerformanceTest {
	
	static String ITEM_KEY_FILE = "";
	static String PAR_KEY_FILE = "";
	static String NN_ADDR = "";
	static int PORT = 0;
	static int BUFFER_SIZE = 50000;
	static int POOL_SIZE = 20;
	
	public static void testEnumerateItem() throws IOException, TEItemEnumeratorException {
		
		FileReader keyFile = new FileReader(PAR_KEY_FILE);
		BufferedReader br = new BufferedReader(keyFile);
		String parkey = null;
		TEClient client = new TEClient(NN_ADDR, PORT);
		long itemCount = 0;
		int parCount = 0;
		long timer = System.currentTimeMillis();

		while ((parkey = br.readLine()) != null) {
			TEItemEnumerator enu = client.getItemEnumerator(parkey);
			while (enu.next()) {
				itemCount++;
			}
			enu.close();
			parCount++;
		}
		
		timer = System.currentTimeMillis() - timer;
		client.close();
		br.close();
		keyFile.close();
		
		System.out
				.println(String
						.format(
								"It takes %s seconds to enumerate %s items from %s partitions.",
								(double)timer / 1000, itemCount, parCount));
	}

	public static void testReadSortedItem() throws IOException, TEClientException {
		
		FileReader fr = new FileReader(ITEM_KEY_FILE);
		BufferedReader br = new BufferedReader(fr);
		String itemKey = null;
		TEClient client = new TEClient(NN_ADDR, PORT);
		long itemCount = 0;
		ArrayList<String> keyList = new ArrayList<String>();
		long timer = System.currentTimeMillis();

		while ((itemKey = br.readLine()) != null) {
			keyList.add(itemKey.trim());
			itemCount++;
			if (itemCount % BUFFER_SIZE == 0) {
				System.out
						.println(String.format("%s items read...", itemCount));
				readItemList(keyList, client);
				keyList.clear();
			}
		}
		System.out.println(String.format("%s items read...", itemCount));

		if (keyList.size() > 0)
			readItemList(keyList, client);
		client.close();
		br.close();
		fr.close();
		timer = System.currentTimeMillis() - timer;

		System.out.println(String.format("It takes %s seconds to read %s items.",
				(double)timer / 1000, itemCount));
	}
	
	public static void testReadRandomItem() throws IOException, TEClientException {
		
		int testTimes = 50;
		int queryPages = 30;
		double[] results = new double[queryPages];
		
		int totalCount = 1238206;

		// get all the item keys
		System.out.println("Loading all the item key...");

		for (int i = 0; i < testTimes; i++) {

			System.out.println(String.format("%s test starting...", i));

			TEClient client = new TEClient(NN_ADDR, PORT);
			
			// random choose 50000 items
			Random r = new Random();
			ArrayList<Integer> lineNumList = new ArrayList<Integer>();
			while(lineNumList.size() < BUFFER_SIZE) {
				lineNumList.add(r.nextInt(totalCount));
			}
			
			Collections.sort(lineNumList);

			ArrayList<String> bufferedKeyList = new ArrayList<String>();
			int lineCount = 0;
			int object = lineNumList.get(bufferedKeyList.size());
			String itemKey = null;

			FileReader fr = new FileReader(ITEM_KEY_FILE);
			BufferedReader br = new BufferedReader(fr);
			while ((itemKey = br.readLine()) != null) {
				if(lineCount == object) {
					bufferedKeyList.add(itemKey.trim());
					if(lineNumList.size() == bufferedKeyList.size())
						break;
					object = lineNumList.get(bufferedKeyList.size());
					while(object == lineCount) {
						bufferedKeyList.add(itemKey.trim());
						object = lineNumList.get(bufferedKeyList.size());
					}
				}
				lineCount ++;
			}
			br.close();
			fr.close();
			
			System.out
					.println(String
							.format(
									"One drill down result with %s items are randomlly choosen.",
									bufferedKeyList.size()));

			for (int j = 0; j < queryPages; j++) {
				// choose the item keys to query
				ArrayList<String> keyList = new ArrayList<String>();
				while (keyList.size() < POOL_SIZE) {
					keyList.add(bufferedKeyList.get(r.nextInt(BUFFER_SIZE)));
				}
				long timer = System.currentTimeMillis();
				readItemList(keyList, client);
				timer = System.currentTimeMillis() - timer;
				results[j] += timer;
			}
			
			client.close();
		}

		System.out.println("The average time cost of query pages:");
		for (int i = 0; i < queryPages; i++) {
//			System.out.println(String.format("It takes %s seconds to query post from the %s page.",((double)results[i]/(1000 * testTimes)), (i+1)));
			System.out.print(i);
			System.out.print("\t");
			System.out.print((double)results[i]/(1000 * testTimes));
			System.out.println();
		}
	}
	
	private static void readItemList(ArrayList<String> keyList, TEClient client) throws TEClientException{
		for(String itemKey : keyList) {
			client.getItem(itemKey);
		}
	}
	
	public static void generateParKeyFile() throws IOException, DecoderException{
		
		FileReader fr = new FileReader(ITEM_KEY_FILE);
		BufferedReader br = new BufferedReader(fr);
		String itemKey = null;
		ArrayList<String> parKeyList = new ArrayList<String>();
		while((itemKey = br.readLine()) != null) {
			ItemKey key = ItemKey.decodeKey(itemKey.trim());
			String parkey = key.getPartitionKey();
			if(!parKeyList.contains(parkey)) {
				parKeyList.add(parkey);
			}
		}
		
		br.close();
		fr.close();
		
		FileWriter fw = new FileWriter(PAR_KEY_FILE);
		PrintWriter pw = new PrintWriter(fw);
		int count = 0;
		for(String parkey: parKeyList) {
			pw.println(parkey);
			count ++;
			if(count % BUFFER_SIZE == 0)
				pw.flush();
		}
		pw.close();
		fw.close();
	}
	
	/**
	 * @param args
	 * @throws TEItemEnumeratorException 
	 * @throws IOException 
	 * @throws TEClientException 
	 * @throws DecoderException 
	 */
	public static void main(String[] args) throws IOException, TEItemEnumeratorException, TEClientException, DecoderException {
		if(args.length < 5) {
			System.out.println("5 parameters needed: NN_ADDR, PORT, ITEM_KEY_FILE, PAR_KEY_FILE, CAL_PAR_FILE");
			return;
		}
		
		NN_ADDR = args[0];
		PORT = Integer.parseInt(args[1]);
		ITEM_KEY_FILE = args[2];
		PAR_KEY_FILE = args[3];
		boolean calculatePar = Boolean.parseBoolean(args[4]);
		
		if(calculatePar)
			generateParKeyFile();
		testEnumerateItem();
		testReadSortedItem();
		testReadRandomItem();
	}

}
