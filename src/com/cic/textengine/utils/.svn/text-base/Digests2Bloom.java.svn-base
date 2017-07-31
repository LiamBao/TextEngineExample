package com.cic.textengine.utils;

import java.io.*;

import com.cic.textengine.type.TEItem;

public class Digests2Bloom {

	/* 从.MD5 文件重建Bloom */
	public static void recreateBloomFromMD5(String md5Filename,
			String removeFilename, String bloomFilename) throws IOException {

		FileInputStream md5Stream = new FileInputStream(md5Filename);
		FileInputStream removeFilestream = new FileInputStream(removeFilename);

		int md5FileSize = md5Stream.available();
		if ((md5FileSize % 16) != 0)
			System.out.println("Waring: md5 fileSize = " + md5FileSize
					+ ", is not multiples of 16.");

		int removeFileSize = removeFilestream.available();
		if ((removeFileSize % 16) != 0)
			System.out.println("Waring: remove fileSize = " + removeFileSize
					+ ", is not multiples of 16.");

		Counting16Filter<TEItem> bloomFilter = (Counting16Filter<TEItem>) new BloomFilterFactory<TEItem>()
				.create(BloomFilterHelper.FILTER_TYPE_COUNTING16,
						BloomFilterHelper.BLOOMFILTER_BIT_SIZE,
						BloomFilterHelper.EXPECTED_ITEM_SIZE);

		int itemCount = 0;
		int errorCount = 0;
		byte[] itemDigest = new byte[16];
		for (int i = 0; i < md5Stream.available() / 16; i++) {
			md5Stream.read(itemDigest);
			long itemHashCode = BloomFilterHelper.readLong(itemDigest, 0);

			if (bloomFilter.contains(itemHashCode)) {
				errorCount++;
				if ((errorCount % 100) == 0)
					System.out.println("Running... itemCount=" + itemCount
							+ "   errorCount=" + errorCount);
				continue;
			}
			bloomFilter.add(itemHashCode);
			itemCount++;
			if ((itemCount % 1000) == 0)
				System.out.println("Running... itemCount=" + itemCount
						+ "   errorCount=" + errorCount);
		}

		int removeCount = 0;
		itemDigest = new byte[16];
		for (int i = 0; i < removeFilestream.available() / 16; i++) {
			removeFilestream.read(itemDigest);
			long itemHashCode = BloomFilterHelper.readLong(itemDigest, 0);
			boolean result = bloomFilter.remove(itemHashCode);
			if (result)
				removeCount++;
		}

		FileOutputStream ostream = new FileOutputStream(bloomFilename);
		ObjectOutputStream p = new ObjectOutputStream(ostream);
		p.writeObject(bloomFilter);
		p.close();
		ostream.close();

		System.out.println("Convert end. md5FileSize=" + md5FileSize
				+ ",  itemCount=" + itemCount + "   errorCount=" + errorCount
				+ "    removeCount=" + removeCount);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length < 3)
		{
			System.out.println("Three parameters needed: md5file removefile bloomfile");
			System.exit(1);
		}
		
		try {
			recreateBloomFromMD5(args[0], args[1], args[2]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
