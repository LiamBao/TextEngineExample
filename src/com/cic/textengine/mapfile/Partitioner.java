package com.cic.textengine.mapfile;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cic.data.ItemMeta;
import com.cic.textengine.config.Configurer;

public class Partitioner {
	private static String PARTITIONPATH_FORMAT = "%s/%d/%d-%d/%s"; 
	public static String getPartitionPath(FileSystem fs, String textRepoPath, ItemMeta meta) throws IOException {
		
		long siteID = meta.getSiteID();
		int year = meta.getYearOfPost();
		int month = meta.getMonthOfPost();
		String forumID = meta.getForumID();
		forumID = new sun.misc.BASE64Encoder().encode( forumID.getBytes()); 
		String dirName = String.format(PARTITIONPATH_FORMAT, textRepoPath, siteID, year, month, forumID);
		
		Path path = new Path(dirName);
		if (!fs.equals(path))
			fs.mkdirs(path);
		return dirName;
	}
}
