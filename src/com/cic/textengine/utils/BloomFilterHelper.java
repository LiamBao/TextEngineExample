package com.cic.textengine.utils;

import java.io.*;

import org.apache.log4j.Logger;
import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;

public class BloomFilterHelper {
	public static final String FILTER_TYPE_COUNTING16 = "Counting16";
	public static final int EXPECTED_ITEM_SIZE = 200000000;
	public static final int BLOOMFILTER_BIT_SIZE = 2000000000;
	private static Logger logger = Logger.getLogger(BloomFilterHelper.class);
	private static PartitionKey currentParKey = null;
	private static Counting16Filter<TEItem> filter = null;
	private static OutputStream digestsOutStream = null;
	private static OutputStream digestsOutStreamRem = null;
	private static int digestsOut_count = 0;
	private static int digestsRem_count = 0;
	private static String BLORA_ITEM_TYPE = "blog";

	/**
	 * To check if the item is duplicated within the partition.
	 * 
	 * @param feedURL
	 */
	public static boolean isDuplicated(PartitionKey parKey, TEItem item) {
		
		// Check if the bloom needs to switch
		boolean needSwitch = needSwitchBloom(item);
		// If bloom needs to switch, serialize the current one and de-serialize the needed one
		if(needSwitch){
			// serialize first
			if(filter != null) {
				try {
					serialize(currentParKey, filter);
				} catch (Exception e) {
					logger.error(String.format("Error serializing current bloom filter"));
				}
			}
			
			// de-serialize
			filter = null;
			digestsOutStream = null;
			digestsOut_count = 0;
			digestsOutStreamRem = null;
			digestsRem_count = 0;
			try {
				filter = deSerialize(parKey);
			} catch (Exception e) {
				logger.error(String.format("Error deserializing bloom filter for partition."));
				return false;
			}
			currentParKey = parKey;
		}
		
		byte[] itemDigest = item.digest();
		long itemHashCode = readLong(itemDigest, 0);;
		
		boolean isDuplicated = false;
		isDuplicated = filter.contains(itemHashCode);
		if (!isDuplicated) {
			try {
				digestsOutStream.write(itemDigest);
				digestsOut_count++;
				if ((digestsOut_count % 1000) == 0)
					digestsOutStream.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			filter.add(itemHashCode);
		} else {
//			logger.debug(String.format("Item with hashcode %s is duplicated.", itemHashCode));
		}
		
		return isDuplicated;
	}
	
   public static long readLong(byte[] b, int off) {
    	return ((b[off + 7] & 0xFFL) << 0) +
    	       ((b[off + 6] & 0xFFL) << 8) +
    	       ((b[off + 5] & 0xFFL) << 16) +
    	       ((b[off + 4] & 0xFFL) << 24) +
    	       ((b[off + 3] & 0xFFL) << 32) +
    	       ((b[off + 2] & 0xFFL) << 40) +
    	       ((b[off + 1] & 0xFFL) << 48) +
    	       ((b[off + 0] & 0xFFL) << 56);
    }


	/**
	 * 序列化操作 将类写入文件
	 * 
	 * @throws Exception
	 */
//	public static void serialize(PartitionKey parKey,
//			BloomFilter<TEItem> bloomFilter) throws Exception {
//		int year = parKey.getYear();
//		serialize(year, bloomFilter);
//	}

// For data before Aug. 2009 use the yearly bloomfilter
// For data after Aug. 2009, use the monthly bloomfilter
	private static void serialize(PartitionKey parKey, BloomFilter<TEItem> bloomFilter) {
		String path = null;
		if(parKey.getSiteID().toLowerCase().startsWith(BLORA_ITEM_TYPE)) {
			path = Configurer.getBloomFilterPath() + "/" + BLORA_ITEM_TYPE;
		} else {
			if(parKey.getYear() < 2009)
				path = Configurer.getBloomFilterPath() + "/" + parKey.getYear();
			else {
				if(parKey.getYear() == 2009 && parKey.getMonth() < 9)
					path = Configurer.getBloomFilterPath() + "/" + parKey.getYear();
				else
					path = Configurer.getBloomFilterPath() + "/" + parKey.getYear()+"_"+parKey.getMonth();
			}
		}

		// logger.info("write:" + path);
		FileOutputStream ostream;
		try {
			ostream = new FileOutputStream(path);
			ObjectOutputStream p = new ObjectOutputStream(ostream);// 绑定

			p.writeObject(bloomFilter);
			p.close();
			ostream.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// 构造文件输出流
 catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.info("Succeeded writing bloomfilter at " + path);
	}

	/**
	 * 序列化操作 将文件读取并转化为类的实例
	 * 
	 * @throws Exception
	 */
	public static Counting16Filter<TEItem> deSerialize(PartitionKey parKey)
			throws Exception {
//		String digestsFileName = Configurer.getBloomFilterPath() + "/" + parKey.getYear()+".md5";
//		File digestsFile = new File(digestsFileName);
//		digestsOutStream = new FileOutputStream(digestsFile, digestsFile.exists());
//		
//		String digestsRemFileName = Configurer.getBloomFilterPath()+"/"+parKey.getYear()+".remove";
//		File digestsRemFile = new File(digestsRemFileName);
//		digestsOutStreamRem = new FileOutputStream(digestsRemFile, digestsRemFile.exists());
		
		String path = null;
		String digestsFileName = null;
		File digestsFile = null;
		String digestsRemFileName = null;
		File digestsRemFile = null;
		if(parKey.getSiteID().toLowerCase().startsWith(BLORA_ITEM_TYPE))
		{
			path = Configurer.getBloomFilterPath() + "/" + BLORA_ITEM_TYPE;
			
			digestsFileName = path+".md5";
			digestsFile = new File(digestsFileName);
			digestsOutStream = new FileOutputStream(digestsFile, digestsFile.exists());
			
			digestsRemFileName = path+".remove";
			digestsRemFile = new File(digestsRemFileName);
			digestsOutStreamRem = new FileOutputStream(digestsRemFile, digestsRemFile.exists());
		}
		else {
			//path = Configurer.getBloomFilterPath() + "/" + parKey.getYear();
			if(parKey.getYear() < 2009)
				path = Configurer.getBloomFilterPath() + "/" + parKey.getYear();
			else {
				if(parKey.getYear() == 2009 && parKey.getMonth() < 9)
					path = Configurer.getBloomFilterPath() + "/" + parKey.getYear();
				else
					path = Configurer.getBloomFilterPath() + "/" + parKey.getYear()+"_"+parKey.getMonth();
			}
			
			digestsFileName = path+".md5";
			digestsFile = new File(digestsFileName);
			digestsOutStream = new FileOutputStream(digestsFile, digestsFile.exists());
			
			digestsRemFileName = path+".remove";
			digestsRemFile = new File(digestsRemFileName);
			digestsOutStreamRem = new FileOutputStream(digestsRemFile, digestsRemFile.exists());
		}

		FileInputStream istream = null;
		File bloomFile = null;
		bloomFile = new File(path);
		if (bloomFile.exists()) {
			Counting16Filter<TEItem> bloomFilter = null;
			try {
				istream = new FileInputStream(bloomFile);
				ObjectInputStream pr = new ObjectInputStream(istream); // 绑定
				bloomFilter = (Counting16Filter<TEItem>) pr.readObject();
				pr.close();
				istream.close();
			} catch (FileNotFoundException e) {
				logger.error(String.format(
						"Error locating bloom filter file %s.", path));
				throw e;
			} catch (IOException e) {
				logger.error(String.format(
						"Error reading bloom filter file %s.", path));
				throw e;
			} catch (ClassNotFoundException e) {
				logger.error(String.format(
						"Error casting deserialized bloom filter file %s.",
						path));
				throw e;
			}

			logger.info("Successfully reading existing bloomfilter file at "
					+ path);

			return bloomFilter;
		} else {
			logger.info("Creating new bloomfilter.");
			Counting16Filter<TEItem> bloomFilter = (Counting16Filter<TEItem>) 
				new BloomFilterFactory<TEItem>()
					.create(FILTER_TYPE_COUNTING16, BLOOMFILTER_BIT_SIZE,
							EXPECTED_ITEM_SIZE);
			logger.info("Expecting false positive rate: "
					+ bloomFilter.expectedFalsePositiveProbability());
			return bloomFilter;
		}
	}

	public static void close() throws Exception {
		if(filter != null) {
			try {
				serialize(currentParKey, filter);
				
				digestsOutStream.flush();
				digestsOutStream.close();
				
				digestsOutStreamRem.flush();
				digestsOutStreamRem.close();
				
				currentParKey = null;
				filter = null;
			} catch(Exception e)
			{
				throw e;
			}
		}
	}
	
	public static void flush() throws IOException
	{
		if(digestsOutStream != null)
			digestsOutStream.flush();
		if(digestsOutStreamRem != null)
			digestsOutStreamRem.flush();
	}
	
	public static boolean remove(PartitionKey parKey, TEItem item)
	{
		// Use Specific Bloom Filter for blora data
		if (item.getMeta().getSource().equalsIgnoreCase(BLORA_ITEM_TYPE)) 
		{
			if(currentParKey == null || !currentParKey.getSiteID().toLowerCase().startsWith(BLORA_ITEM_TYPE))
			{
				// first serialize current bloom filter
				if(filter != null) {
					try {
						serialize(currentParKey, filter);
					} catch (Exception e) {
						logger.error(String.format("Error serializing current bloom filter"));
					}
				}
				filter = null;
				// load the blora filter
				filter = null;
				digestsOutStream = null;
				digestsOut_count = 0;
				digestsOutStreamRem = null;
				digestsRem_count = 0;
				try {
					filter = deSerialize(parKey);
				} catch (Exception e) {
					logger.error(String.format("Error deserializing bloom filter for partition."));
					return false;
				}
				currentParKey = parKey;
			}
		} else {
		// for FF & SERA data
			if ((currentParKey == null)
					|| currentParKey.getSiteID().toLowerCase().startsWith(BLORA_ITEM_TYPE) 
					|| (parKey.getYear() != currentParKey.getYear()) || (parKey.getMonth() != currentParKey.getMonth()) ) {
				/* first serialize current bloom filter */
				if (filter != null)
					try {
						serialize(currentParKey, filter);
					} catch (Exception e) {
						logger.error(String.format("Error serializing current bloom filter"));
					}
				/* load new bloom filter */
				filter = null;
				try {
					/* first serialize current bloom filter */
					if (digestsOutStream != null)
					{
						digestsOutStream.flush();
						digestsOutStream.close();
					}
	
					if (digestsOutStreamRem != null)
					{
						digestsOutStreamRem.flush();
						digestsOutStreamRem.close();
					}
					
					if (filter != null)
						serialize(currentParKey, filter);
					
					/* load new bloom filter */
					filter = null;
					digestsOutStream = null;
					digestsOut_count = 0;
					digestsOutStreamRem = null;
					digestsRem_count = 0;
					filter = deSerialize(parKey);
				} catch (Exception e) {
					logger.error(String.format("Error deserializing bloom filter for partition."));
					return false;
				}
				currentParKey = parKey;
			}
		}
		
		byte[] itemDigest = item.digest();
		long itemHashCode = readLong(itemDigest, 0);
		
		boolean result = filter.remove(itemHashCode);
		if(result)
		{
			try {
				digestsOutStreamRem.write(itemDigest);
				digestsRem_count++;
				if ((digestsRem_count % 100) == 0)
					digestsOutStreamRem.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return result;
	}
	
	private static boolean needSwitchBloom(TEItem item){
		boolean needSwitch = false;
		
		// see if the item is a blog
		if (item.getMeta().getSource().equalsIgnoreCase(BLORA_ITEM_TYPE)){
			if(currentParKey == null || !currentParKey.getSiteID().toLowerCase().startsWith(BLORA_ITEM_TYPE)){
				// current parkey is empty or current bloom is not for blora
				needSwitch = true;
			}
			return needSwitch;
		} else {
			// for FF & SERA data
			if ((currentParKey == null)|| currentParKey.getSiteID().toLowerCase().startsWith(BLORA_ITEM_TYPE)){
				// current parkey = empty or current bloom is for blora
				needSwitch = true;
			} else{
				int itemYear = item.getMeta().getYearOfPost();
				int itemMonth = item.getMeta().getMonthOfPost();
				
				if((itemYear < 2009 || itemYear ==2009 && itemMonth < 9) && itemYear == currentParKey.getYear()){
					// for data before Sep. 2009, check if the year of item and year of partition is the same.
					needSwitch = false;
				} else if(currentParKey.getMonth() == itemMonth && currentParKey.getYear() == itemYear){
					// for data after Sep. 2009, check if the year and month of item is the same with the year and month of partition.
					needSwitch = false;
				} else {
					needSwitch = true;
				}
			}
			
			return needSwitch;
		}
	}
}
