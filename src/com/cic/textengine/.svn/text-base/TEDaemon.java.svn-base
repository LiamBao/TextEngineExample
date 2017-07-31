package com.cic.textengine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.cic.data.Item;
import com.cic.data.ItemMeta;
import com.cic.data.impl.BaseItemMetaImpl;
import com.cic.textengine.config.Configurer;
import com.cic.textengine.idgenarator.IDGenerator;
import com.cic.textengine.itemcollector.ItemCollector;
import com.cic.textengine.itemcollector.StdOutItemPrinter;
import com.cic.textengine.itemreader.ItemReader;
import com.cic.textengine.itemreader.SnapShot;
import com.cic.textengine.itemreader.XMLItemReader;
import com.cic.textengine.segmentprocessor.BlockMerger;
import com.cic.textengine.segmentprocessor.ItemIndexer;
import com.cic.textengine.segmentprocessor.SegmentPrinter;
import com.cic.textengine.segmentprocessor.SegmentProcessor;

public class TEDaemon {
	private static final int OPTION_WRITE = 1;

	private static final int OPTION_READ = 2;

	private static final int OPTION_TRAVERSE = 3;

	private static final int OPTION_MERGEBLOCK = 4;

	private static final int OPTION_INDEX = 5;

	private static final int OPTION_PRINTOUT = 6;

	private static final int OPTION_OPTIMIZEINDEX = 7;

	private static final int OPTION_READBYITEMID = 8;
	
	private static final Logger logger = Logger.getLogger(TEDaemon.class);

	public static void main(String[] args) {

		if (args.length == 0) {
			System.out.println("Usage: <config> <write|traverse> [targetPath]");
			return;
		}

		// BasicConfigurator.configure();
		String cfgFile = args[0];
		String sOption = args[1];

		Configurer.config(cfgFile);

		Configuration conf = new Configuration();
		try {
			// FileSystem fs = FileSystem.getLocal(conf);
			TextEngine te = TextEngine.getInstance(conf);
			

			int option = 0;
			if (sOption.equals("write")) {
				option = OPTION_WRITE;
			}
			if (sOption.equals("traverse")) {
				option = OPTION_TRAVERSE;
			}
			if (sOption.equals("mergeblock")) {
				option = OPTION_MERGEBLOCK;
			}
			if (sOption.equals("index")) {
				option = OPTION_INDEX;
			}
			if (sOption.equals("print")) {
				option = OPTION_PRINTOUT;
			}
			if (sOption.equals("optimizeindex")) {
				option = OPTION_OPTIMIZEINDEX;
			}
			if (sOption.equals("readbyitemid")) {
				option = OPTION_READBYITEMID;
			}

			if (option == OPTION_WRITE) {
				SnapShot.recover();
				String sStart = "-1";
				String itemPath = null;
				String textRepoPath = null;
				if (args.length < 5) {
					logger
							.debug("Please specify the location of items, text repository, and the start id");
					return;
				}
				itemPath = args[2];
				textRepoPath = args[3];
				sStart = args[4];

				ItemReader reader = null;
				reader = new XMLItemReader(itemPath);

				long start = Long.valueOf(sStart);

				if (start < 0) {
					long lastItemID = SnapShot.loadLastItemID();
					if (lastItemID != 0)
						/*
						 * only when no init id is given, which means to resume
						 * the previous writing, the id within idsnap will take
						 * effect.
						 */
						start = lastItemID + Configurer.getBatchSize();
					else {
						logger.debug("Please give a valid init item id");
						return;
					}
				}
				
				logger.debug("Write to text repository at " + textRepoPath);
				IDGenerator.getInstance(start);
				te.write(textRepoPath, reader);

				logger.debug("Writing of items finished.");
			}
			if (option == OPTION_READ) {
				String itemPath = null;
				String textRepoPath = null;
				if (args.length < 4) {
					logger
							.debug("Please specify the location of text repository");
					return;
				}

				itemPath = args[2];
				textRepoPath = args[3];
				ItemReader reader = null;
				reader = new XMLItemReader(itemPath);
				te.search(textRepoPath, reader);
			}
			
			if (option == OPTION_READBYITEMID) {
				String itemPath = null;
				String textRepoPath = null;
				if (args.length < 4) {
					logger
							.debug("Please specify the location of text repository");
					return;
				}

				textRepoPath = args[2];
				long itemID = Long.valueOf(args[3]);
				ItemMeta meta = new BaseItemMetaImpl();
				meta.setItemID(itemID);
				List<ItemMeta> metas = new ArrayList<ItemMeta>();
				metas.add(meta);
				List<Item> items = te.readItems(textRepoPath, metas);
				for (Item item : items) {
					logger.debug("Retrieved itemID: " + item.getMeta().getItemID());
					logger.debug("Retrieved content: " + item.getContent());
				}
			}
			
			if (option == OPTION_TRAVERSE) {
				String textRepoPath = null;
				if (args.length < 3) {
					logger
							.debug("Please specify the location of text repository");
					return;
				}

				textRepoPath = args[2];
				ItemCollector itemCollector = new StdOutItemPrinter();
				te.traverseItems(textRepoPath, itemCollector);
			}
			if (option == OPTION_MERGEBLOCK) {
				String textRepoPath = null;
				if (args.length < 3) {
					logger
							.debug("Please specify the location of text repository");
					return;
				}

				textRepoPath = args[2];
				logger.debug("Start to merge blocks");
				FileSystem fs = FileSystem.get(conf);
				SegmentProcessor blockMerger = new BlockMerger(fs, conf);
				te.processSegments(textRepoPath, blockMerger);
				logger.debug("Merging of items finished.");
			}
			if (option == OPTION_INDEX) {
				String textRepoPath = null;
				String indexRepoPath = null;
				if (args.length < 4) {
					return;
				}

				textRepoPath = args[2];
				indexRepoPath = args[3];
				FileSystem fs = FileSystem.get(conf);
				SegmentProcessor itemIndexer = new ItemIndexer(fs, conf,
						indexRepoPath);
				te.processSegments(textRepoPath, itemIndexer);
			}
			if (option == OPTION_PRINTOUT) {
				FileSystem fs = FileSystem.get(conf);
				SegmentPrinter printer = new SegmentPrinter(fs, conf);

				if (args.length < 3) {
					System.out.println("Usage: print <target> <fields>");
					return;
				}

				String target = args[2];

				String sFields = args[3];
				printer.setFields(sFields);

				Configurer.config(cfgFile);

				te.processSegments(printer, target);
				
				logger.debug("Indexing of items finished.");
			}

			if (option == OPTION_OPTIMIZEINDEX) {
				String dest = args[2];
				File destPath = new File(dest);
				IndexWriter destIndex = null;
				try {
					if (!destPath.exists()) {
						destIndex = new IndexWriter(dest, null, true);
					} else {
						destIndex = new IndexWriter(dest, null, false);
					}

					destIndex.optimize();
					logger.debug(String.format(
							"Indexes %s merged successfully ", destPath));
					destIndex.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
