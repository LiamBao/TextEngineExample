package com.cic.textengine.partitionprocessor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import com.cic.textengine.TextRepositoryImporter;
import com.cic.textengine.config.Configurer;
import com.cic.textengine.mapfile.MapFileManager;
import com.cic.textengine.type.PartitionInfo;
import com.cic.textengine.type.SegmentInfo;

public class PartitionImporter implements PartitionProcessor {
	private FileSystem srcFs = null;

	private FileSystem dstFs = null;

	private String srcDir = null;

	private String dstDir = null;

	private Configuration dstHdpConf = null;

	private static String PARTITIONNAME_FORMAT = "%s/%s/%s/%s";

	private static final Logger logger = Logger
			.getLogger(PartitionImporter.class);
	
	private boolean forceOverwrite = true;

	public PartitionImporter(FileSystem srcFs, FileSystem dstFs, String srcDir,
			String dstDir, Configuration dstHdpConf) {
		this.srcFs = srcFs;
		this.dstFs = dstFs;
		this.srcDir = srcDir;
		this.dstDir = dstDir;
		this.dstHdpConf = dstHdpConf;
	}

	public void process(String srcPartition) throws IOException {
		Path srcPartitionPath = new Path(srcPartition);
		String forum = srcPartitionPath.getName();
		Path monthPath = srcPartitionPath.getParent();
		String month = monthPath.getName();
		Path sitePath = monthPath.getParent();
		String site = sitePath.getName();

		String dstPartition = String.format(PARTITIONNAME_FORMAT, dstDir, site, month,
				forum);
		Path dstPartitionPath = new Path(dstPartition);
		Path dstPartitionInfoFile = new Path(dstPartitionPath, "partitioninfo");
		// Path dstSegmentInfoFile = new Path(dstPartitionPath, "segmentinfo");

		if (dstFs.exists(dstPartitionInfoFile)) {
			logger.debug(dstPartitionInfoFile + " exists");

			List<SegmentInfo> srcSegmentInfos = MapFileManager.readSegmentInfo(
					srcFs, srcPartition);
			List<SegmentInfo> dstSegmentInfos = MapFileManager.readSegmentInfo(
					dstFs, dstPartition);

			if (dstSegmentInfos.size() > 0 && srcSegmentInfos.get(0).equalsRange(dstSegmentInfos.get(0))) {
				/* The 2 partitions are the same, shall not override */
				logger.debug(String.format("%s is identical to %s, skip.",
						srcPartition, dstPartition));
				return;
			}

			PartitionInfo srcPartitionInfo = MapFileManager.readPartitionInfo(
					srcFs, srcPartition);
			PartitionInfo dstPartitionInfo = MapFileManager.readPartitionInfo(
					dstFs, dstPartition);

			List<SegmentInfo> newSegmentInfos = new ArrayList<SegmentInfo>();
			for (SegmentInfo srcSegmentInfo : srcSegmentInfos) {
				srcSegmentInfo.setTag(0);
				newSegmentInfos.add(srcSegmentInfo);
				logger.debug(String.format("will import src %s:%s:%s", srcSegmentInfo.getSegmentID(), srcSegmentInfo.getStart(), srcSegmentInfo.getEnd()));
			}
			for (SegmentInfo dstSegmentInfo : dstSegmentInfos) {
				dstSegmentInfo.setTag(1);
				newSegmentInfos.add(dstSegmentInfo);
			}

			/* resort the segment info */
			Collections.sort(newSegmentInfos, new Comparator<SegmentInfo>() {
				public int compare(SegmentInfo o1, SegmentInfo o2) {
					long start1 = o1.getStart();
					long start2 = o2.getStart();
					if (start1 < start2)
						return -1;
					if (start1 == start2)
						return 0;
					return 1;
				}
			});

			/* map the former segment id to the new id after merging */
			Map<Integer, Integer> dstIDMap = new HashMap<Integer, Integer>();
			Map<Integer, Integer> srcIDMap = new HashMap<Integer, Integer>();
			for (int i = 0; i < newSegmentInfos.size(); i++) {
				SegmentInfo segmentInfo = newSegmentInfos.get(i);
				if (segmentInfo.getTag() == 0) {
					srcIDMap.put(segmentInfo.getSegmentID(), i);
				}
				if (segmentInfo.getTag() == 1) {
					dstIDMap.put(segmentInfo.getSegmentID(), i);
				}

				segmentInfo.setSegmentID(i);
			}
			logger.debug("Update segmentinfo at " + dstPartition);
			MapFileManager
					.saveSegmentInfo(dstFs, dstPartition, newSegmentInfos);

			/* update partition info */
			int newActiveSegmentSize = 0;
			int newNumOfSegments = newSegmentInfos.size();
			if (newSegmentInfos.get(newNumOfSegments - 1).getTag() == 0) {
				newActiveSegmentSize = srcPartitionInfo.getActiveSize();
			} else {
				newActiveSegmentSize = dstPartitionInfo.getActiveSize();
			}
			PartitionInfo newPartitionInfo = new PartitionInfo();
			newPartitionInfo.setActiveSize(newActiveSegmentSize);
			newPartitionInfo.setNumOfSegments(newNumOfSegments);
			logger.debug("Update partition at " + dstPartition);
			MapFileManager.savePartitionInfo(dstFs, dstPartition,
					newPartitionInfo);

			Path[] srcSegmentPaths = srcFs.listPaths(srcPartitionPath);
			Path[] dstSegmentPaths = dstFs.listPaths(dstPartitionPath);
			/* first to rename the existing segments on destination, if necessary */
			for (Path dstSegmentPath : dstSegmentPaths){
				if (dstFs.isFile(dstSegmentPath)) {
					continue;
				}
				int oldSegmentID = Integer.valueOf(dstSegmentPath.getName());
				int newSegmentID = dstIDMap.get(oldSegmentID);			
				if (oldSegmentID != newSegmentID){
					Path newDstSegmentPath = new Path(dstPartitionPath, newSegmentID + "");
					logger.debug(String.format("Rename segment from %s to %s",
							dstSegmentPath, newDstSegmentPath));
					dstFs.rename(dstSegmentPath, newDstSegmentPath);
				}
			}
			for (Path srcSegmentPath : srcSegmentPaths) {
				if (srcFs.isFile(srcSegmentPath)) {
					continue;
				}
				int oldSegmentID = Integer.valueOf(srcSegmentPath.getName());
				int newSegmentID = srcIDMap.get(oldSegmentID);
				Path dstSegmentPath = new Path(dstPartitionPath, newSegmentID
						+ "");
				logger.debug(String.format("Copy segment from %s to %s",
						srcSegmentPath, dstSegmentPath));
				try {
					TextRepositoryImporter.update(srcFs, srcSegmentPath, dstFs,
						dstSegmentPath, false, dstHdpConf);
				}
				catch (IOException ex) {
					logger.error(String.format("Failure copying segment from %s to %s",
							srcSegmentPath, dstSegmentPath));
				}
			}
		} else {
			logger.debug(String.format("Copy partition from %s to %s",
					srcPartitionPath, dstPartitionPath));
			TextRepositoryImporter.update(srcFs, srcPartitionPath, dstFs,
					dstPartitionPath, false, dstHdpConf);
		}
	}

	public static void main(String[] args) {
		String cfgFile = args[0];
		String srcConfPath = args[1];
		String srcPath = args[2];
		String dstConfPath = args[3];
		String dstPath = args[4];

		Configurer.config(cfgFile);

		Configuration srcHdpConf = new Configuration();
		Configuration dstHdpConf = new Configuration();
		dstHdpConf.addDefaultResource(dstConfPath + File.separator
				+ "hadoop-default.xml");
		dstHdpConf.addFinalResource(dstConfPath + File.separator
				+ "hadoop-site.xml");

		try {
			FileSystem srcFs = FileSystem.get(srcHdpConf);
			FileSystem dstFs = FileSystem.get(dstHdpConf);

			PartitionImporter importer = new PartitionImporter(srcFs, dstFs,
					srcPath, dstPath, dstHdpConf);
			MapFileManager man = new MapFileManager(srcHdpConf, srcFs);
			man.processPartition(importer, srcPath);
			logger.debug("Partition importing finished.");

		} catch (IOException e) {
			logger.error(e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
