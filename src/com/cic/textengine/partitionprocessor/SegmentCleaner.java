package com.cic.textengine.partitionprocessor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cic.textengine.config.Configurer;
import com.cic.textengine.mapfile.MapFileManager;
import com.cic.textengine.type.SegmentInfo;

/**
 * SegmentationCleaner is to first remove all remote segments which will be overwritten
 * by local segments. 
 * 
 * This class shall always be used together with PartitionImporter, otherwise 
 * information in partitioninfo will not be correct, as the change of segments 
 * will not be updated on partitioninfo.
 *  
 * @author textd
 *
 */
public class SegmentCleaner implements PartitionProcessor {
	private FileSystem srcFs = null;

	private FileSystem dstFs = null;

	private String srcDir = null;

	private String dstDir = null;

	private Configuration dstHdpConf = null;

	private static String PARTITIONNAME_FORMAT = "%s/%s/%s/%s";

	private static final Logger logger = Logger
			.getLogger(PartitionImporter.class);

	private boolean forceOverwrite = true;

	public SegmentCleaner(FileSystem srcFs, FileSystem dstFs,
			String srcDir, String dstDir, Configuration dstHdpConf) {
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

		String dstPartition = String.format(PARTITIONNAME_FORMAT, dstDir, site,
				month, forum);
		Path dstPartitionPath = new Path(dstPartition);
		Path dstPartitionInfoFile = new Path(dstPartitionPath, "partitioninfo");
		
		if (dstFs.exists(dstPartitionInfoFile)) {
			logger.debug(dstPartitionInfoFile + " exists");

			List<SegmentInfo> srcSegmentInfos = MapFileManager.readSegmentInfo(
					srcFs, srcPartition);
			List<SegmentInfo> dstSegmentInfos = MapFileManager.readSegmentInfo(
					dstFs, dstPartition);
			
			List<SegmentInfo> newDstSegmentInfos = new ArrayList<SegmentInfo>();
			for (SegmentInfo dstSegmentInfo : dstSegmentInfos) {
				newDstSegmentInfos.add(dstSegmentInfo);
			}
			
			/*
			 * To check if there are remote segments to be overwritten
			 */
			for (int i = 0; i < srcSegmentInfos.size(); i++) {
				SegmentInfo srcSegmentInfo = srcSegmentInfos.get(i);
				for (int j = 0; j < dstSegmentInfos.size(); j++) {
					SegmentInfo dstSegmentInfo = dstSegmentInfos.get(j);
					if (srcSegmentInfo.equalsRange(dstSegmentInfo)) {
						if (forceOverwrite) {
							Path dstSegmentPath = new Path(dstPartitionPath,
									dstSegmentInfo.getSegmentID() + "");
							Path srcSegmentPath = new Path(srcPartitionPath,
									srcSegmentInfo.getSegmentID() + "");
							logger.debug(dstSegmentPath
									+ "will be overwritten by "
									+ srcSegmentPath);
							 dstFs.delete(dstSegmentPath);
							 newDstSegmentInfos.remove(dstSegmentInfo);
						}
					}
				}
			}
			
			/*
			 * To update segmentinfo, rename remaining segments if necessary.
			 */
			for (int i=0; i<newDstSegmentInfos.size(); i++){
				SegmentInfo newDstSegmentInfo = newDstSegmentInfos.get(i);
				int segID = newDstSegmentInfo.getSegmentID();
				if (segID != i) {
					Path oldDstSegmentPath = new Path(dstPartitionPath, segID + "");
					segID = i;
					newDstSegmentInfo.setSegmentID(segID);
					Path newDstSegmentPath = new Path(dstPartitionPath, segID + "");
					logger.debug("Change name of " + oldDstSegmentPath + " to " + newDstSegmentPath);
					dstFs.rename(oldDstSegmentPath, newDstSegmentPath);
				}
			}
			logger.debug("Update segmentinfo at " + dstPartition);
			MapFileManager.saveSegmentInfo(dstFs, dstPartition, newDstSegmentInfos);
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

			SegmentCleaner cleaner = new SegmentCleaner(srcFs, dstFs,
					srcPath, dstPath, dstHdpConf);
			MapFileManager man = new MapFileManager(srcHdpConf, srcFs);
			man.processPartition(cleaner, srcPath);
			logger.debug("Partition importing finished.");

		} catch (IOException e) {
			logger.error(e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
