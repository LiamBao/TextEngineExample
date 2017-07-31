package com.cic.textengine.segmentprocessor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import com.cic.textengine.type.TEItem;

public class BlockMerger implements SegmentProcessor {
	private FileSystem fs = null;

	private Configuration conf = null;

	private static Logger logger = Logger
			.getLogger(BlockMerger.class.getName());

	public BlockMerger(FileSystem fs, Configuration conf) {
		this.fs = fs;
		this.conf = conf;
	}

	public void process(String segment) throws IOException {
		Path segmentPath = new Path(segment);
		Path blocks[] = fs.listPaths(segmentPath);

		if (blocks.length == 1){
			logger.debug("No need to merge within segment " + segment);
			return;
		}

		/* write items into one new block */
		String tmpBlockPath = segment + File.separator + "tmp";
		logger.debug("Merge into block " + tmpBlockPath);
		MapFile.Writer writer = new MapFile.Writer(conf, fs, tmpBlockPath,
				LongWritable.class, TEItem.class,
				SequenceFile.CompressionType.BLOCK);

		/* sort the blocks */
		Arrays.sort(blocks, 
			new Comparator<Path>() {
				public int compare(Path blk1Path, Path blk2Path){
					String blk1 = blk1Path.toString();
					String blk2 = blk2Path.toString();

					int lastSlashIdx1 = blk1.lastIndexOf("/");		
					int lastSlashIdx2 = blk2.lastIndexOf("/");		
					String sBlkID1 = blk1.substring(lastSlashIdx1+1, blk1.length());
					String sBlkID2 = blk2.substring(lastSlashIdx2+1, blk2.length());
					int blkID1 = Integer.valueOf(sBlkID1);
					int blkID2 = Integer.valueOf(sBlkID2);
					return blkID1 - blkID2;
				}	
			}
		);

		/* read items out of existing blocks */
		for (Path block : blocks) {
			if (!fs.isDirectory(block))
				continue;

			MapFile.Reader reader = new MapFile.Reader(fs, block.toString(),
					conf);
			logger.debug("Reading from block " + block);
			LongWritable key = new LongWritable();
			TEItem item = new TEItem();
			try {
				while (reader.next(key, item)) {
					writer.append(key, item);
					key = new LongWritable();
					item = new TEItem();
				}
			} catch (IOException e) {
				logger.error("Reading aborted from block " + block);
			}
			reader.close();

			/* remove old blocks */
			fs.delete(block);
		}

		writer.close();

		/* rename the tmp block */
		String newBlockPath = segment + File.separator + 1;
		fs.rename(new Path(tmpBlockPath), new Path(newBlockPath));
	}

	public void mergeBlock(String segment) throws IOException {
		process(segment);
	}
	
	public void close(){
		
	}
}
