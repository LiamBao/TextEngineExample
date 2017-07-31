package com.cic.textengine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class TextRepositoryImporter {
	private static final Logger logger = Logger.getLogger(TextRepositoryImporter.class);
	
	public static void main(String[] args) {
		if (args.length < 4) {
			System.out
					.println("Usage: TextRepositoryImporter RepositoryName SourceConf DestConf");
			return;
		}

		BasicConfigurator.configure();
		
		String srcConfPath = args[0];
		String srcPath = args[1];
		String dstConfPath = args[2];
		String dstPath = args[3];

		Configuration srcHdpConf = new Configuration();
		Configuration dstHdpConf = new Configuration();
		dstHdpConf.addDefaultResource(dstConfPath + File.separator
				+ "hadoop-default.xml");
		dstHdpConf.addFinalResource(dstConfPath + File.separator
				+ "hadoop-site.xml");

		try {
			FileSystem srcFs = FileSystem.get(srcHdpConf);
			FileSystem dstFs = FileSystem.get(dstHdpConf);
			
			update(srcFs, new Path(srcPath), dstFs, new Path(dstPath), false, dstHdpConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * To check if the destination path already exists as a file.
	 * @param srcName The source path
	 * @param dstFS The destination file system
	 * @param dst The destition path
	 * @return
	 * @throws IOException if the destination path already exists as a file.
	 */
	private static Path checkDest(String srcName, FileSystem dstFS, Path dst)
			throws IOException {
		if (dstFS.exists(dst)) {
			if (!dstFS.isDirectory(dst)) {
				throw new IOException("Target " + dst + " already exists");
			} else {
				// dst = new Path(dst, srcName);
				// if (dstFS.exists(dst)) {
				// throw new IOException("Target " + dst + " already exists");
				// }
			}
		}
		return dst;
	}

	private static void copyContent(InputStream in, OutputStream out,
			Configuration conf) throws IOException {
		copyContent(in, out, conf, true);
	}

	private static void copyContent(InputStream in, OutputStream out,
			Configuration conf, boolean close) throws IOException {
		byte buf[] = new byte[conf.getInt("io.file.buffer.size", 4096)];
		try {
			int bytesRead = in.read(buf);
			while (bytesRead >= 0) {
				out.write(buf, 0, bytesRead);
				bytesRead = in.read(buf);
			}
		} finally {
			if (close)
				out.close();
		}
	}

	//
	// If the destination is a subdirectory of the source, then
	// generate exception
	//
	private static void checkDependencies(FileSystem srcFS, Path src,
			FileSystem dstFS, Path dst) throws IOException {
		if (srcFS == dstFS) {
			String srcq = srcFS.makeQualified(src).toString() + Path.SEPARATOR;
			String dstq = dstFS.makeQualified(dst).toString() + Path.SEPARATOR;
			if (dstq.startsWith(srcq)) {
				if (srcq.length() == dstq.length()) {
					throw new IOException("Cannot copy " + src + " to itself.");
				} else {
					throw new IOException("Cannot copy " + src
							+ " to its subdirectory " + dst);
				}
			}
		}
	}

	/**
	 * update the dest FileSystem by copying the new files from the source
	 * FileSystem.
	 */
	public static boolean update(FileSystem srcFS, Path src, FileSystem dstFS,
			Path dst, boolean deleteSource, Configuration conf)
			throws IOException {
		dst = checkDest(src.getName(), dstFS, dst);

		if (srcFS.isDirectory(src)) {
			checkDependencies(srcFS, src, dstFS, dst);
			if (!dstFS.mkdirs(dst)) {
				return false;
			}
			Path contents[] = srcFS.listPaths(src);
			for (int i = 0; i < contents.length; i++) {
				update(srcFS, contents[i], dstFS, new Path(dst, contents[i]
						.getName()), deleteSource, conf);
			}
		} else if (srcFS.isFile(src)) {
			InputStream in = srcFS.open(src);
			try {
				OutputStream out = dstFS.create(dst);
				logger.debug(String.format("Copy from %s to %s", src, dst));
				copyContent(in, out, conf);
			} finally {
				in.close();
			}
		} else {
			throw new IOException(src.toString()
					+ ": No such file or directory");
		}
		if (deleteSource) {
			return srcFS.delete(src);
		} else {
			return true;
		}

	}
}
