package com.cic.textengine.mapfile;

import java.util.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.streaming.UTF8ByteArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.cic.data.Item;
import com.cic.data.ItemMeta;
import com.cic.textengine.itemcollector.ItemCollector;
import com.cic.textengine.itemcollector.ListItemCollector;
import com.cic.textengine.itemreader.SnapShot;
import com.cic.textengine.partitionprocessor.PartitionProcessor;
import com.cic.textengine.segmentprocessor.SegmentProcessor;
import com.cic.textengine.type.PartitionInfo;
import com.cic.textengine.type.SegmentInfo;
import com.cic.textengine.type.TEItemMeta;
import com.cic.textengine.type.TEItem;

public class MapFileManager {
	private static final int MAP_SIZE = 500000;

	private static String PARTITIONINFO_FORMAT = "%d:%d";

	private static String SEGMENTINFO_FORMAT = "%d:%d:%d";

	private static Logger logger = Logger.getLogger(MapFileManager.class);

	private static String newLine = (String) java.security.AccessController
			.doPrivileged(new sun.security.action.GetPropertyAction(
					"line.separator"));

	/**
	 * Map the path to a current available MapFile writer. There shall be at
	 * most one open writer for each path at any time.
	 */

	/**
	 * Map the egment file name to the corresponding MapFile reader.
	 */
	// private Map<String, MapFile.Reader> segment2SegmentReader = new
	// HashMap<String, MapFile.Reader>();
	/**
	 * Record the number of mapfiles under the given partition path The numbe
	 * gets updated when a new file is created or an old file is removed.
	 */
	private Map<String, PartitionInfo> partition2PartitionInfo = new HashMap<String, PartitionInfo>();

	/**
	 * Record the segementInfo list for the given path. The range list gets
	 * updated when a new file is created or an old file is removed.
	 */
	private Map<String, List<SegmentInfo>> partition2SegmentInfoList = new HashMap<String, List<SegmentInfo>>();

	private Configuration conf = null;

	private FileSystem fs = null;

	public MapFileManager(Configuration conf, FileSystem fs) throws IOException {
		this.conf = conf;
		this.fs = fs;
		logger.setLevel(Level.DEBUG);
	}

	public void close() throws IOException {
		// activeSegmentWriterPool.removeAll();
	}

	/**
	 * This function has potential by-product effect to update the partition
	 * info if a new active segment is created.
	 * 
	 * @param partitionPath
	 * @return
	 * @throws Exception
	 */
	private Writer getActiveSegmentWriter(String partitionPath)
			throws Exception {
		// MapFile.Writer writer =
		// activeSegmentWriterPool.borrow(partitionPath);
		MapFile.Writer writer = null;
		PartitionInfo partitionInfo = getPartitionInfo(partitionPath);
		/*
		 * First to check if the size of the active segment has reached the
		 * limit,
		 */
		if (partitionInfo.getActiveSize() < MAP_SIZE) {
			if (writer == null) {
				int activeSegmentID = partitionInfo.getNumOfSegments() - 1;
				activeSegmentID = activeSegmentID >= 0 ? activeSegmentID : 0;
				String activeSegmentPath = partitionPath + File.separator
						+ activeSegmentID;

				/*
				 * since each singal mapfile is not mutable, segment is further
				 * broken down to blocks
				 */
				Path blocks[] = fs.listPaths(new Path(activeSegmentPath));
				int blockID = blocks.length + 1;

				String activeBlockPath = activeSegmentPath + File.separator
						+ blockID;
				fs.mkdirs(new Path(activeBlockPath));

				writer = new MapFile.Writer(conf, fs, activeBlockPath,
						LongWritable.class, TEItem.class,
						SequenceFile.CompressionType.BLOCK);
				// activeSegmentWriterPool.add(partitionPath, writer);
			}
		} else {
			/* if yes, create a new segment and make it active */
			partitionInfo.setActiveSize(0);
			partitionInfo.increaseNumOfSegments();
			int activeSegmentID = partitionInfo.getNumOfSegments();
			String activeSegmentPath = partitionPath + File.separator
					+ activeSegmentID;
			int blockID = 1;
			String activeBlockPath = activeSegmentPath + File.separator
					+ blockID;

			writer = new MapFile.Writer(conf, fs, activeBlockPath,
					LongWritable.class, TEItem.class,
					SequenceFile.CompressionType.BLOCK);
			// activeSegmentWriterPool.add(partitionPath, writer);
		}

		return writer;
	}

	/**
	 * Get the number of mapfiles listed under the given path
	 * 
	 * @param partitionPath
	 * @return
	 * @throws IOException
	 */
	private PartitionInfo getPartitionInfo(String partitionPath)
			throws IOException {
		PartitionInfo info = partition2PartitionInfo.get(partitionPath);
		if (info == null) {
			info = readPartitionInfo(fs, partitionPath);
			/* If the partition info file exist */
			if (info == null) {
				info = new PartitionInfo();
			}
			partition2PartitionInfo.put(partitionPath, info);
		}
		return info;
	}

	private static String getPartitionInfoFileName(String partitionPath) {
		String infoFileName = partitionPath + File.separator + "partitioninfo";
		return infoFileName;
	}

	private List<SegmentInfo> getSegmentInfo(String partitionPath)
			throws IOException {
		List<SegmentInfo> infoList = partition2SegmentInfoList
				.get(partitionPath);
		if (infoList == null) {
			infoList = readSegmentInfo(fs, partitionPath);
			partition2SegmentInfoList.put(partitionPath, infoList);
		}
		return infoList;
	}

	private static String getSegmentInfoFileName(String partitionPath) {
		String segmentInfoFileName = partitionPath + File.separator
				+ "segmentinfo";
		return segmentInfoFileName;
	}

	/**
	 * Read items according to their itemMetas
	 * 
	 * @param itemMetas
	 * @return
	 * @throws IOException
	 */
	public List<Item> readItemsFromRepo(String textRepo, List<ItemMeta> itemMetas) throws IOException {
		List<Item> items = new ArrayList<Item>();
		Map<String, List<ItemMeta>> partition2MetaQueue = new HashMap<String, List<ItemMeta>>();

		/* split itemMetas into queues of different partition */
		for (ItemMeta itemMeta : itemMetas) {
			String partition = Partitioner.getPartitionPath(fs, textRepo, itemMeta);
			List<ItemMeta> metaQueue = partition2MetaQueue.get(partition);
			if (metaQueue == null) {
				metaQueue = new ArrayList<ItemMeta>();
				partition2MetaQueue.put(partition, metaQueue);
			}
			logger.debug(String.format("Group [itemid:%s siteid:%s year:%s month:%s forumid:%s] into %s", itemMeta.getItemID(), itemMeta.getSiteID(), itemMeta.getYearOfPost(), itemMeta.getMonthOfPost(), itemMeta.getForumID(), partition));
			metaQueue.add(itemMeta);
		}
		/* read items based on itemMeta queues */
		for (String partition : partition2MetaQueue.keySet()) {
			List<ItemMeta> metas = partition2MetaQueue.get(partition);
			Collections.sort(metas);
			logger.debug(String.format("Send %d queries to partition %s", metas
					.size(), partition));
			List<Item> subItems;
			try {
				subItems = readItemsFromPartition(partition, metas);
				logger.debug(String.format("Return %d hits from partition %s",
						subItems.size(), partition));
				items.addAll(subItems);
			} catch (IOException e) {
				/* Partition level exception, skip to next partition */
				logger.debug(String.format(
						"Fail to read hits from partition %s", partition));
			}
		}
		return items;
	}

	/**
	 * Read from a Mapfile reader a list of items according to a list of
	 * pre-sorted itemMetas
	 * 
	 * @param reader
	 * @param sortedItemIDs
	 * @return
	 * @throws IOException
	 */
	private void readItems(MapFile.Reader reader,
			List<ItemMeta> sortedItemMetas, ItemCollector collector)
			throws IOException {
		LongWritable key = new LongWritable();
		for (ItemMeta itemMeta : sortedItemMetas) {
			long itemID = itemMeta.getItemID();
			TEItem item = new TEItem();
			key.set(itemID);
			if (reader.seek(key)) {
				reader.get(key, item);
				item.setMeta(itemMeta);
				collector.collect(item);
			}
		}
	}

	/**
	 * Read every key and value from a map file
	 * 
	 * @param reader
	 * @param collector
	 * @throws IOException
	 */
	private void readItems(MapFile.Reader reader, ItemCollector collector)
			throws IOException {
		LongWritable key = new LongWritable();
		TEItem item = new TEItem();
		while (reader.next(key, item)) {
			collector.collect(item);
		}
	}

	/**
	 * Read items from specified partition
	 * 
	 * @param partitionPath
	 * @param sortedItemMetas
	 * @return
	 * @throws IOException
	 * @throws IOException
	 *             if the entire partition is not read correctly.
	 */
	private List<Item> readItemsFromPartition(String partitionPath,
			List<ItemMeta> sortedItemMetas) throws IOException {
		List<Item> items = new ArrayList<Item>();
		List<SegmentInfo> segmentInfos = getSegmentInfo(partitionPath);
		
		long lastEnd = 0;
		for (SegmentInfo segmentInfo : segmentInfos) {
			
			/* The mechnism to deal with problem that one segmentinfo may have more than one same blocks, 
			 *  2007.10.27 
			 * Paul Wang
			 */
			if (segmentInfo.getStart() <= lastEnd) 
				break;
			lastEnd = segmentInfo.getEnd();
			
			TEItemMeta keyStart = new TEItemMeta();
			keyStart.setItemID(segmentInfo.getStart());

			TEItemMeta keyEnd = new TEItemMeta();
			keyEnd.setItemID(segmentInfo.getEnd());

			logger.debug(String.format("keyStart: %d, keyEnd: %d", keyStart.getItemID(), keyEnd.getItemID()));
			logger.debug(String.format("metaStart: %d, metaEnd: %d", sortedItemMetas.get(0).getItemID(), sortedItemMetas.get(sortedItemMetas.size()-1).getItemID()));
			int idxStart = Collections.binarySearch(sortedItemMetas, keyStart);
			int idxEnd = Collections.binarySearch(sortedItemMetas, keyEnd);
			
			if (idxStart < -1 || idxEnd == -1)
				continue;

			if (idxEnd < -1) {
				idxEnd = -2 - idxEnd;
			}
			if (idxStart < -1) {
				idxStart = -2 - idxStart;
			}
			if (idxStart == -1) {
				idxStart = 0;
			}

			List<ItemMeta> candidates = sortedItemMetas.subList(idxStart,
					idxEnd + 1);

			String segmentPath = partitionPath + File.separator
					+ segmentInfo.getSegmentID();

			logger.debug(String.format("----Send %d queries to segment %s",
					candidates.size(), segmentPath));

			Path blocks[] = null;
			/* segment level exception handling */
			try {
				blocks = fs.listPaths(new Path(segmentPath));
				for (Path block : blocks) {
					if (!fs.isDirectory(block))
						continue;
					MapFile.Reader reader = null;
					/* block level exception handling */
					try {
						reader = new MapFile.Reader(fs, block.toString(), conf);
						ListItemCollector collector = new ListItemCollector();
						readItems(reader, candidates, collector);
						List<Item> subItems = collector.getItems();
						logger.debug(String.format(
								"---------Return %d hits from block %s",
								subItems.size(), block));
						items.addAll(subItems);
					} catch (IOException e) {
						logger.error(String.format(
								"---------Fail to return hits from block %s",
								block));
					} finally {
						if (reader != null) {
							reader.close();
							reader = null;
						}
					}
				}
			} catch (IOException e) {
				/* Segment level exception, skip to next segment */
				logger.error(String.format("----Error reading segment %s",
						segmentPath));
			}

			logger.debug(String.format("----Return %d hits from segment %s",
					items.size(), segmentPath));
			if (items.size() != candidates.size())
				logger.debug(String.format(
						"----Hits not complete for segment %s", segmentPath));
		}
		return items;
	}

	private PartitionInfo readLocalPartitionInfo(String partitionPath)
			throws IOException {
		PartitionInfo info = null;
		String infoFileName = getPartitionInfoFileName(partitionPath);
		BufferedReader reader = null;
		try {
			File infoFile = new File(infoFileName);
			if (infoFile.exists()) {
				reader = new BufferedReader(new FileReader(infoFileName));
				String infoRecord = reader.readLine();
				String[] infoArray = infoRecord.split(":");
				info = new PartitionInfo();
				info.setNumOfSegments(Integer.valueOf(infoArray[0]));
				info.setActiveSize(Integer.valueOf(infoArray[1]));
			}
		} catch (FileNotFoundException e) {
			// logger.debug("Failed to find " + infoFileName);
		} catch (IOException e) {
			logger.error("Unexpected reading error: " + e.getMessage());
		} finally {
			if (reader != null) {
				reader.close();
				reader = null;
			}
		}

		return info;
	}

	public static PartitionInfo readPartitionInfo(FileSystem fs,
			String partitionPath) throws IOException {
		PartitionInfo info = null;
		String infoFileName = getPartitionInfoFileName(partitionPath);

		FSDataInputStream input = null;
		try {
			Path infoFilePath = new Path(infoFileName);
			if (fs.exists(infoFilePath)) {
				input = fs.open(infoFilePath);
				String infoRecord = new String(UTF8ByteArrayUtils
						.readLine(input));
				String[] infoArray = infoRecord.split(":");
				info = new PartitionInfo();
				info.setNumOfSegments(Integer.valueOf(infoArray[0]));
				info.setActiveSize(Integer.valueOf(infoArray[1]));
			}
		} catch (FileNotFoundException e) {
			// logger.debug("Failed to find " + infoFileName);
		} catch (IOException e) {
			logger.error("Unexpected reading error: " + e.getMessage());
		} finally {
			if (input != null) {
				input.close();
				input = null;
			}
		}

		return info;
	}

	public static List<SegmentInfo> readSegmentInfo(FileSystem fs,
			String partitionPath) throws IOException {
		List<SegmentInfo> infoList = new ArrayList<SegmentInfo>();

		String segmentInfoFileName = getSegmentInfoFileName(partitionPath);

		FSDataInputStream input = null;
		try {
			Path infoFilePath = new Path(segmentInfoFileName);
			if (fs.exists(infoFilePath)) {
				input = fs.open(infoFilePath);
				byte[] line = null;
				while ((line = UTF8ByteArrayUtils.readLine(input)) != null) {
					String[] sgArr = new String(line).split(":");
					int sgID = Integer.valueOf(sgArr[0]);
					int sgStart = Integer.valueOf(sgArr[1]);
					int sgEnd = Integer.valueOf(sgArr[2]);
					SegmentInfo info = new SegmentInfo();
					info.setSegmentID(sgID);
					info.setStart(sgStart);
					info.setEnd(sgEnd);
					infoList.add(info);
				}
			}
		} catch (FileNotFoundException e) {
			// logger.debug("Failed to find " + segmentInfoFileName);
		} catch (NumberFormatException e) {
			logger.error("Unexpected reading error: " + e.getMessage());
		} catch (IOException e) {
			logger.error("Unexpected reading error: " + e.getMessage());
		} finally {
			if (input != null) {
				input.close();
				input = null;
			}
		}

		return infoList;
	}

	private List<SegmentInfo> readLocalSegmentInfo(String partitionPath)
			throws IOException {
		List<SegmentInfo> infoList = new ArrayList<SegmentInfo>();

		String segmentInfoFileName = getSegmentInfoFileName(partitionPath);
		BufferedReader reader = null;
		try {
			File infoFile = new File(segmentInfoFileName);
			if (infoFile.exists()) {
				reader = new BufferedReader(new FileReader(segmentInfoFileName));
				String line = null;
				while ((line = reader.readLine()) != null) {
					String[] sgArr = line.split(":");
					int sgID = Integer.valueOf(sgArr[0]);
					int sgStart = Integer.valueOf(sgArr[1]);
					int sgEnd = Integer.valueOf(sgArr[2]);
					SegmentInfo info = new SegmentInfo();
					info.setSegmentID(sgID);
					info.setStart(sgStart);
					info.setEnd(sgEnd);
					infoList.add(info);
				}
			}
		} catch (FileNotFoundException e) {
			// logger.debug("Failed to find " + segmentInfoFileName);
		} catch (NumberFormatException e) {
			logger.error("Unexpected reading error: " + e.getMessage());
		} catch (IOException e) {
			logger.error("Unexpected reading error: " + e.getMessage());
		} finally {
			if (reader != null) {
				reader.close();
				reader = null;
			}
		}

		return infoList;
	}

	private void savePartitionInfo() throws IOException {
		for (String partition : partition2PartitionInfo.keySet()) {
			savePartitionInfo(partition);
		}
	}

	private void savePartitionInfo(String partition) throws IOException {
		PartitionInfo info = partition2PartitionInfo.get(partition);
		savePartitionInfo(fs, partition, info);
	}

	public static void savePartitionInfo(FileSystem fs, String partition,
			PartitionInfo info) throws IOException {
		Path dir = new Path(partition);
		if (!fs.exists(dir)) {
			fs.mkdirs(dir);
		}
		String partitionInfoFileName = getPartitionInfoFileName(partition);
		FSDataOutputStream output = null;
		Path partitionInfoPath = new Path(partitionInfoFileName);
		output = fs.create(partitionInfoPath);
		String infoRecord = String.format(PARTITIONINFO_FORMAT, info
				.getNumOfSegments(), info.getActiveSize());
		output.writeBytes(infoRecord + newLine);
		output.close();
	}

	private void saveLocalPartitionInfo(String partition) throws IOException {
		File dir = new File(partition);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		String partitionInfoFileName = getPartitionInfoFileName(partition);
		BufferedWriter writer = new BufferedWriter(new FileWriter(
				partitionInfoFileName, false));
		PartitionInfo info = partition2PartitionInfo.get(partition);
		String infoRecord = String.format(PARTITIONINFO_FORMAT, info
				.getNumOfSegments(), info.getActiveSize());
		writer.append(infoRecord);
		writer.close();
	}

	private void saveActiveSegmentInfo() throws IOException {
		for (String partition : partition2SegmentInfoList.keySet()) {
			saveActiveSegmentInfo(partition);
		}
	}

	private void saveActiveSegmentInfo(String partition) throws IOException {

		Path dir = new Path(partition);
		if (!fs.exists(dir)) {
			fs.mkdirs(dir);
		}

		List<SegmentInfo> infoList = partition2SegmentInfoList.get(partition);

		saveSegmentInfo(fs, partition, infoList);
	}

	public static void saveSegmentInfo(FileSystem fs, String partition,
			List<SegmentInfo> segmentInfos) throws IOException {
		String segmentInfoFileName = getSegmentInfoFileName(partition);
		Path partitionInfoPath = new Path(segmentInfoFileName);
		FSDataOutputStream output = fs.create(partitionInfoPath);
		for (SegmentInfo info : segmentInfos) {
			String record = String.format(SEGMENTINFO_FORMAT, info
					.getSegmentID(), info.getStart(), info.getEnd());
			output.writeBytes(record + newLine);
			logger.debug(String.format("Write %s to segmentinfo", record));
		}
		output.close();
	}

	private void saveLocalActiveSegmentInfo(String partition)
			throws IOException {
		String segmentInfoFileName = getSegmentInfoFileName(partition);
		File dir = new File(partition);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter(
				segmentInfoFileName, false));
		List<SegmentInfo> infoList = partition2SegmentInfoList.get(partition);
		for (SegmentInfo info : infoList) {
			String record = String.format(SEGMENTINFO_FORMAT, info
					.getSegmentID(), info.getStart(), info.getEnd());
			writer.append(record);
			writer.newLine();
		}
		writer.close();
	}

	public void saveStatus() throws IOException {
		// write path2SegmentInfoList;
		saveActiveSegmentInfo();
		// partition2FileCount;
		savePartitionInfo();
	}

	public void writeItemsToRepo(String textRepo, List<TEItem> items) throws Exception {
		Map<String, List<TEItem>> dir2ItemQueue = new HashMap<String, List<TEItem>>();

		/* split itemMetas into queues of different path */
		for (TEItem item : items) {
			String dirName = Partitioner.getPartitionPath(fs, textRepo, item.getMeta());
			List<TEItem> itemQueue = dir2ItemQueue.get(dirName);
			if (itemQueue == null) {
				itemQueue = new ArrayList<TEItem>();
				dir2ItemQueue.put(dirName, itemQueue);
			}
			itemQueue.add(item);
		}
		/* read items based on itemMeta queues */
		for (String path : dir2ItemQueue.keySet()) {
			List<TEItem> subItems = dir2ItemQueue.get(path);
			Collections.sort(subItems);
			writeItemsToPartition(path, subItems);
		}
	}

	/**
	 * Write items with specified MapFile writer from the given starting index
	 * 
	 * @param writer
	 * @param sortedItems
	 *            Starting index
	 * @param start
	 * @return The num of items the MapFile writer acually wrote.
	 * @throws IOException
	 */
	private int writeItems(MapFile.Writer writer, List<TEItem> sortedItems,
			int currentSize, int start) throws IOException {
		int writeSize = 0;
		int idx = start;
		for (; idx < sortedItems.size(); idx++) {
			TEItem item = sortedItems.get(idx);
			if (currentSize + writeSize == MAP_SIZE)
				break;

			long itemID = item.getMeta().getItemID();
			LongWritable key = new LongWritable();
			key.set(itemID);

			try {
				writer.append(key, item);
				// SnapShot.setItemSignature(item.getMeta().getItemUrl());
				SnapShot.setItemSignature(item.getMeta().getHash());
				SnapShot.setItemID(itemID);
			} catch (IOException e) {
				logger.error(String.format(
						"Item %d was not written successfully.", itemID));
				throw e;
			} catch (Exception e) {
				logger.error(String.format(
						"Item %d was not written successfully.", itemID));
				e.printStackTrace();
				System.exit(1);
			}

			writeSize++;
		}
		return writeSize;
	}

	/**
	 * Write items to mapfiles
	 * 
	 * @param partitionPath
	 * @param items
	 * @throws Exception
	 * @throws Exception
	 */
	private void writeItemsToPartition(String partitionPath, List<TEItem> items)
			throws Exception {
		int idx = 0;
		int itemSize = items.size();
		List<SegmentInfo> segmentInfos = getSegmentInfo(partitionPath);
		int lastSegmentIdx = segmentInfos.size() - 1;

		SegmentInfo lastSegment = lastSegmentIdx >= 0 ? segmentInfos
				.get(lastSegmentIdx) : null;
		logger.debug(String.format("To write %d items to partition %s",
				itemSize, partitionPath));
		while (idx <= itemSize - 1) {
			MapFile.Writer writer = getActiveSegmentWriter(partitionPath);
			PartitionInfo partitionInfo = getPartitionInfo(partitionPath);

			/*
			 * To check if the last segment doesn't reach its size limit, then
			 * prepare to modify its end value, otherwise create a new segement
			 * info.
			 */
			SegmentInfo newSegment = null;
			int newSegmentID = partitionInfo.getNumOfSegments();
			newSegmentID = newSegmentID >= 0 ? newSegmentID : 0;
			if (lastSegment != null
					&& lastSegment.getSegmentID() == newSegmentID) {
				newSegment = lastSegment;
			} else {
				newSegment = new SegmentInfo();
				newSegment.setSegmentID(newSegmentID);
				newSegment.setStart(items.get(idx).getMeta().getItemID());
				segmentInfos.add(newSegment);
			}

			int size;
			try {
				size = writeItems(writer, items, partitionInfo.getActiveSize(),
						idx);
				logger.debug(String.format(
						"   Wrote %d items to segment %s/%d", size,
						partitionPath, newSegmentID));
				idx += size;

				partitionInfo.increaseSize(size);
				newSegment.setEnd(items.get(idx - 1).getMeta().getItemID());
				if (size == 0) {
					logger.error("sth is wrong");
				}
			} catch (IOException e) {
				logger.error("Failed to write items due to " + e.getMessage());
				e.printStackTrace();
			} finally {
				releaseActiveSegmentWriter(partitionPath, writer);
			}
		}
	}

	private void releaseActiveSegmentWriter(String partitionPath, Writer writer)
			throws Exception {
		// activeSegmentWriterPool.sendBack(partitionPath, writer);
		writer.close();
		savePartitionInfo(partitionPath);
		saveActiveSegmentInfo(partitionPath);
	}

	private boolean traverseSegment(Path src, SegmentProcessor segmentProcessor)
			throws IOException {
		Path paths[] = fs.listPaths(src);
		if (paths == null) {
			throw new IOException("Could not get listing for " + src);
		}

		Arrays.sort(paths);

		for (int i = 0; i < paths.length; i++) {
			Path cur = paths[i];

			if (fs.isDirectory(cur)) {
				boolean processed = traverseSegment(cur, segmentProcessor);
				if (processed)
					return false;
			} else {
				String subPath = cur.getName();
				if (!(subPath.equals("data") || subPath.equals("index")))
					continue;
				String segment = cur.getParent().getParent().toString();

				/*
				 * To solve a bug in early version of TextEngine, which put
				 * mapfile directly under segment instead of block
				 */
				String wrongData = segment + File.separator + "data";
				String wrongIndex = segment + File.separator + "index";
				if (fs.exists(new Path(wrongData))) {
					logger.debug("Wrong data " + wrongData + " detected");
					String newBlockPath = segment + File.separator + 0;
					fs.rename(new Path(wrongData), new Path(newBlockPath
							+ File.separator + "data"));
					fs.rename(new Path(wrongIndex), new Path(newBlockPath
							+ File.separator + "index"));
				}

				try {
					logger.debug("Enter segment " + segment);
					segmentProcessor.process(segment);
				} catch (IOException e) {
					e.printStackTrace();
					logger.error("Failed to read segment " + segment);
				}
				return true;
			}
		}
		return false;
	}

	private boolean traversePartition(Path src,
			PartitionProcessor partitionProcessor) throws IOException {
		Path paths[] = fs.listPaths(src);
		if (paths == null) {
			throw new IOException("Could not get listing for " + src);
		}

		Arrays.sort(paths, new Comparator<Path>() {
			public int compare(Path o1, Path o2) {
				return o2.compareTo(o1);
			}
		});

		for (int i = 0; i < paths.length; i++) {
			Path cur = paths[i];

			if (fs.isDirectory(cur)) {
				boolean processed = traversePartition(cur, partitionProcessor);
			} else {
				String subPath = cur.getName();
				if (!(subPath.equals("segmentinfo") || subPath
						.equals("partitioninfo")))
					continue;
				String partition = cur.getParent().toString();

				try {
					logger.debug("Enter partition " + partition);
					partitionProcessor.process(partition);
				} catch (IOException e) {
					e.printStackTrace();
					logger.error("Failed to read partition " + partition);
				}
				return true;
			}
		}
		return false;
	}

	private void traverse(Path src, ItemCollector itemCollector)
			throws IOException {
		Path paths[] = fs.listPaths(src);
		if (paths == null) {
			throw new IOException("Could not get listing for " + src);
		}
		for (int i = 0; i < paths.length; i++) {
			Path cur = paths[i];

			if (fs.isDirectory(cur))
				traverse(cur, itemCollector);
			else {
				String subPath = cur.getName();
				if (!(subPath.equals("data") || subPath.equals("index")))
					continue;
				String segment = cur.getParent().toString();
				MapFile.Reader reader = new MapFile.Reader(fs, segment, conf);
				;
				logger.debug("Read from " + segment);
				readItems(reader, itemCollector);
				reader.close();
				return;
			}
		}
	}

	public void traverse(String root, ItemCollector itemCollector) throws IOException {
		Path rootPath = new Path(root);
		traverse(rootPath, itemCollector);
	}

	public void processSegments(String root, SegmentProcessor segmentProcessor)
			throws IOException {
		Path rootPath = new Path(root);

		traverseSegment(rootPath, segmentProcessor);
	}

	public void processSegments(SegmentProcessor segmentProcessor, String base)
			throws IOException {
		Path basePath = new Path(base);
		traverseSegment(basePath, segmentProcessor);
	}

	public void processPartition(PartitionProcessor partitionProcessor,
			String src) throws IOException {
		traversePartition(new Path(src), partitionProcessor);
	}
}
