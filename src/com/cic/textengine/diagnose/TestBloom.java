package com.cic.textengine.diagnose;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.cic.textengine.type.TEItem;
import com.cic.textengine.utils.BloomFilterFactory;
import com.cic.textengine.utils.BloomFilterHelper;
import com.cic.textengine.utils.Counting16Filter;

public class TestBloom {
	
	public static final String FILTER_TYPE_COUNTING16 = "Counting16";
	public static final int EXPECTED_ITEM_SIZE = 200000000;
	public static final int BLOOMFILTER_BIT_SIZE = 2000000000;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length < 1)
		{
			System.out.println("1 parameters needed: hashFile");
			return;
		}
		
		InputStream isMD5 = new FileInputStream(args[0].trim());
		Counting16Filter<TEItem> bf = (Counting16Filter<TEItem>)new BloomFilterFactory<TEItem>().create(FILTER_TYPE_COUNTING16, BLOOMFILTER_BIT_SIZE, EXPECTED_ITEM_SIZE);
		
		int falsePositives = 0;
		int actualAdded = 0;
		int hashCount = 0;
		byte[] bytes = new byte[16];
		
		System.out.println("Begin 1st round add hash.");
		while(isMD5.read(bytes) != -1) {
			long hashCode = BloomFilterHelper.readLong(bytes, 0);
			hashCount ++;
			
			if(bf.contains(hashCode)){
				falsePositives ++;
			} else{
				actualAdded++;
				bf.add(hashCode);
			}
		}
		isMD5.close();
		isMD5 = null;
		System.out.println(String.format("%s items added, %s items are duplicated", actualAdded, falsePositives));
		
		System.out.println("Begin 2nd round add hash.");
		isMD5 = new FileInputStream(args[0].trim());
		falsePositives = 0;
		actualAdded = 0;
		hashCount = 0;
		while(isMD5.read(bytes) != -1) {
			long hashCode = BloomFilterHelper.readLong(bytes, 0);
			hashCount ++;
			
			if(bf.contains(hashCode)){
				falsePositives ++;
			} else{
				actualAdded++;
				bf.add(hashCode);
			}
		}
		System.out.println(String.format("%s items added, %s items are duplicated", actualAdded, falsePositives));

	}

}
