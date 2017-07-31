package com.cic.textengine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import com.cic.data.Item;
import com.cic.data.ItemMeta;
import com.cic.textengine.config.Configurer;
import com.cic.textengine.itemcollector.ItemCollector;
import com.cic.textengine.itemreader.ItemReader;
import com.cic.textengine.mapfile.MapFileManager;
import com.cic.textengine.segmentprocessor.SegmentProcessor;
import com.cic.textengine.type.TEItem;

public class TextEngine {

	private static final Logger logger = Logger.getLogger(TextEngine.class);

	private Configuration hadoopConf = null;

	private FileSystem fs = null;
	
	private MapFileManager manager = null;

	private static TextEngine te = null;

	private TextEngine(Configuration hadoopConf, FileSystem fs, MapFileManager manager) {
		this.hadoopConf = hadoopConf;
		this.fs = fs;
		this.manager = manager;
	}

	public static TextEngine getInstance(Configuration hadoopConf)
			throws IOException {

		if (te == null) {
			FileSystem fs = FileSystem.get(hadoopConf);
			
			MapFileManager manager = new MapFileManager(hadoopConf, fs);
			te = new TextEngine(hadoopConf, fs, manager);
		}
		return te;
	}

	/**
	 * Wrtie items reading from ItemReader into Text Repository
	 * @param reader
	 */
	public void write(String textRepo, ItemReader reader) {
		try {
			int count = 0;
			List<TEItem> items = new ArrayList<TEItem>();
			int size = 0;

			while (reader.next()) { // copy all entries
				Item item = reader.getItem();
				if (item != null) {
					items.add((TEItem) item);
					size++;
					count++;
				}
				if (size == Configurer.getBatchSize()) {
					manager.writeItemsToRepo(textRepo, items);
					items.clear();
					size = 0;
				}
			}
			manager.writeItemsToRepo(textRepo, items);
			items = null;
			reader.close();
			manager.saveStatus();
			manager.close();
		} catch (Exception e) {
			logger.error("Failed to write items due to: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Look for items within TextRepository, given the item metas read from
	 * ItemReader
	 * 
	 * @param manager
	 * @param reader
	 * 
	 */
	public void search(String textRepo, ItemReader reader) {
		try {
			List<Item> items = new ArrayList<Item>();
			List<ItemMeta> itemMetas = new ArrayList<ItemMeta>();
			int size = 0;
			int count = 0;

			while (reader.next()) { // copy all entries
				ItemMeta itemMeta = reader.getItem().getMeta();
				itemMetas.add(itemMeta);
				count++;
				size++;
				if (size == Configurer.getBatchSize()) {
					List<Item> subItems = manager.readItemsFromRepo(textRepo, itemMetas);
					items.addAll(subItems);
					itemMetas.clear();
					size = 0;
					break;
				}
			}
			reader.close();

			System.out.println("Total hits: " + items.size());
			for (Item item : items) {
				System.out.println(item.getMeta().getItemID());
				System.out.println(item.getSubject());
				System.out.println(item.getContent());
			}
		} catch (Exception e) {
			logger.error("Failed to search items due to: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public List<Item> readItems(String textRepo, List<ItemMeta> metas) {
		List<Item> items = null;
		try {
			items = manager.readItemsFromRepo(textRepo, metas);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return items;
	}

	private void read(Configuration conf, FileSystem fs, String in) {
		try {
			MapFile.Reader reader = new MapFile.Reader(fs, in, conf);
			Text key = new Text();
			Text value = new Text();
			long start = System.currentTimeMillis();
			int count = 0;
			while (reader.next(key, value)) {
				System.out.println(key);
				if (count++ == 100000)
					break;
			}
			long end = System.currentTimeMillis();
			System.out.print((end - start));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void incrementRead(Configuration conf, FileSystem fs,
			String in, int end) {
		try {
			MapFile.Reader reader = new MapFile.Reader(fs, in, conf);
			Text key = new Text();
			Text value = new Text();
			long count = 0;
			long start = System.currentTimeMillis();

			long idx = 400000;
			while (idx < 500000) {
				long offset = new Random().nextInt(3000);
				idx += offset;
				Text target = new Text();
				target.set(idx + "");
				while (key.compareTo(target) < 0)
					reader.next(key, value);
				// if (key.compareTo(target) > 0)
				// System.out.println("mismatch " + key + " " + target);
				// else
				// System.out.println("match " + key + " " + target);
				// if (value == null)
				// break;
				count++;
				// System.out.println(key + " " + idx + " " + offset );
			}
			long finish = System.currentTimeMillis();
			System.out.println(finish - start);
			System.out.println(count);
			reader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void reverseRead(Configuration conf, FileSystem fs,
			String in, int end) {
		try {
			MapFile.Reader reader = new MapFile.Reader(fs, in, conf);
			Text key = new Text();
			Text value = new Text();
			long count = 0;
			long start = System.currentTimeMillis();
			for (long i = 10000; i > 0; i--) {
				long z = new Random().nextInt(end);
				key.set(z + "");
				reader.get(key, value);
				// if (value == null)
				// break;
				if (count++ == 1000) {
					break;
				}
				// System.out.println(key + ":" + value);
			}
			long finish = System.currentTimeMillis();
			System.out.println(finish - start);
			reader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void traverseItems(String textRepo, 
			ItemCollector itemCollector) {
		try {
			manager.traverse(textRepo, itemCollector);
			logger.info("Traverse items finished. ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Failed to traverse items due to: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public void processSegments(SegmentProcessor blockMerger, String path) throws IOException {
		manager.processSegments(blockMerger, path);
	}

	public void processSegments(String textRepo, SegmentProcessor blockMerger) throws IOException {
		manager.processSegments(textRepo, blockMerger);
	}
}
