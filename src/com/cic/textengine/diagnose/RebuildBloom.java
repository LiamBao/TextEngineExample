package com.cic.textengine.diagnose;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;

import com.cic.textengine.type.TEItem;
import com.cic.textengine.utils.BloomFilterFactory;
import com.cic.textengine.utils.BloomFilterHelper;
import com.cic.textengine.utils.Counting16Filter;

public class RebuildBloom {
	
	static InputStream isMD5 = null;
	static InputStream isRemove = null;
	static Counting16Filter<TEItem> filter = null;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length < 2){
			System.out.println("2 parameters needed: bloomname, path");
			return;
		}
		String bloomName = args[0].trim();
		String path = args[1].trim();
		isMD5 = new FileInputStream(path+bloomName+".md5");
		isRemove = new FileInputStream(path+bloomName+".remove");
		filter =(Counting16Filter<TEItem>)new BloomFilterFactory<TEItem>()
		.create(BloomFilterHelper.FILTER_TYPE_COUNTING16, BloomFilterHelper.BLOOMFILTER_BIT_SIZE,
				BloomFilterHelper.EXPECTED_ITEM_SIZE);
		byte[] bytes = new byte[16];
		while(isMD5.read(bytes) != -1) {
			long hashCode = BloomFilterHelper.readLong(bytes, 0);
			filter.add(hashCode);
		}
		while(isRemove.read(bytes) != -1) {
			long hashCode = BloomFilterHelper.readLong(bytes, 0);
			filter.remove(hashCode);
		}
		
		FileOutputStream fos = new FileOutputStream(path+bloomName);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(filter);
		oos.close();
		fos.close();
	}

}
