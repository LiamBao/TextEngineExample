package com.cic.textengine.repository.importer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.log4j.Logger;

import com.cic.textengine.datadelivery.DcmisDB;
import com.cic.textengine.repository.importer.exception.ProcessIsolationException;

public class ProcessIsolateXML implements ImporterProcess {
	static final int MAX_ISOLATE_XML_COUNT = 100;
	File m_XMLFolder_Source = null;
	File m_XMLFolder_Isolution = null;

	public ProcessIsolateXML(File srcPath, File isolationPath) {
		m_XMLFolder_Source = srcPath;
		m_XMLFolder_Isolution = isolationPath;
	}

	Logger m_logger = Logger.getLogger(ProcessIsolateXML.class);

	public void process(ItemImporterPerformanceLogger perfLogger)
			throws ProcessIsolationException {
		m_logger.info("Isolating XML...");
		
		try {
			DcmisDB.createConnection();
		} catch (Exception e2) {
			m_logger.error("Fail to connect to dcmis database.");
			throw new ProcessIsolationException(e2);
		}

		File[] sourceDirs = m_XMLFolder_Source.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.isDirectory();
			}
		});
		ArrayList<File[]> filearraylist = new ArrayList<File[]>();
		int totallength = 0;
		for (int i = 0; i < sourceDirs.length; i++) {
			File[] tempfiles = sourceDirs[i].listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					name = name.toLowerCase();
					if (name.endsWith(".xml") || name.endsWith(".gz")) {
						return true;
					} else {
						return false;
					}
				}
			});
			filearraylist.add(tempfiles);
			totallength += tempfiles.length;
		}
		File[] files = new File[totallength];
		int currentpos = 0;
		for (int i = 0; i < filearraylist.size(); i++) {
			File[] tempfiles = filearraylist.get(i);
			System.arraycopy(tempfiles, 0, files, currentpos, tempfiles.length);
			currentpos += tempfiles.length;
		}

		File[] processingFiles = m_XMLFolder_Isolution
				.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String name) {
						if (name.toLowerCase().endsWith(".xml")) {
							return true;
						} else {
							return false;
						}
					}
				});

		int count = 0;
		Date deletionDate = null;
		try {
			deletionDate = getDeletionDate();
		} catch (Exception e1) {
			m_logger.error("Fail to obtain the deletion info.");
			DcmisDB.close();
			throw new ProcessIsolationException(e1);
		}
		if (processingFiles.length == 0) {
			String destPath = m_XMLFolder_Isolution.getAbsolutePath();

			File src = null;
			File dest = null;

			// sort the file according to the last modified time
			Arrays.sort(files, new FileCompareByDate());

			// isolate files: 1) less the the MAX_ISOLATE_XML_COUNT. 2) earlier
			// than the deletion operation
			for (int i = 0; i < files.length; i++) {
				if (count >= MAX_ISOLATE_XML_COUNT)
					break;
				src = files[i];
				dest = new File(destPath + File.separator + src.getName());
				if (deletionDate != null) {
					Date fileDate = new Date(src.lastModified());
					if (fileDate.after(deletionDate))
						break;
				}
				try {
					moveSourceToDest(src, dest);
				} catch (IOException e) {
					m_logger.error(String.format("Error copying %s to %s", src,
							dest));
					DcmisDB.close();
					throw new ProcessIsolationException(e);
				}

				count++;

			}

			// check if there are more files early before the deletion
			// operation.
			// If so, reset the deletion operation.
			if (count < files.length && deletionDate != null) {
				Date nextFileDate = new Date(files[count].lastModified());
				if (nextFileDate.before(deletionDate))
					try {
						resetDeletionOperation();
						deletionDate = null;
					} catch (Exception e) {
						m_logger.error("Fail to reset the deletion operation.");
						DcmisDB.close();
						throw new ProcessIsolationException(e);
					}
			}
			
			if(deletionDate != null) {
				try {
					generateDeleteLog();
				} catch (Exception e) {
					m_logger.error("Fail to log the deletion operation.");
					DcmisDB.close();
					throw new ProcessIsolationException(e);
				}
			}
			m_logger.info("  " + count
					+ " XML files are isolated for processing.");

			if (files.length <= 0) {
				try {
					Thread.currentThread().sleep(10000);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		} else {
			count = processingFiles.length;
			m_logger
					.info("  "
							+ processingFiles.length
							+ " XML files exist in the isolation area, process them first. ");
		}
		
		DcmisDB.close();
	}

	private void moveSourceToDest(File src, File dest) throws IOException {
		boolean deleteSrc = true;
		
		// Create channel on the source
		FileChannel srcChannel = new FileInputStream(src).getChannel();

		// Create channel on the destination
		FileChannel dstChannel = new FileOutputStream(dest).getChannel();

		// Copy file contents from source to destination
		dstChannel.transferFrom(srcChannel, 0, srcChannel.size());
		
		// Test if there are more bytes from the source
		if(srcChannel.position() != srcChannel.size())
			deleteSrc = false;

		// Close the channels
		srcChannel.close();
		dstChannel.close();

		// Remove the source file
		if(deleteSrc) {
			src.delete();
			m_logger.info(String.format("File %s moved to %s", src, dest));
		} else {
			// remove the dest file
			dest.delete();
			m_logger.info("More bytes to copy");
			throw new IOException("There are more bytes in the source file to copy.");
		}
	}

	private Date getDeletionDate() throws Exception {
		Date date = null;
		Timestamp time = null;
		DcmisDB.createConnection();
		Connection conn = DcmisDB.getConnection();
		String sql = "select * from T_DELETION where STATUS = 0 and TYPE = 1 order by ID ASC";
		Statement stat = conn.createStatement();
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		if (result.next()) {
			time = result.getTimestamp("submit_time");
			date = new Date(time.getTime());
		}
		if (date != null) {
			sql = "update T_DELETION set STATUS = '1' where STATUS = '0' and TYPE = '1' and SUBMIT_TIME = ?";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setTimestamp(1, time);
			ps.execute();
			ps.close();
		}
		result.close();
		stat.close();
		return date;
	}
	
	private void generateDeleteLog() throws Exception {
		
		String logFileName = "TE_Item_Delete_"+System.currentTimeMillis()+".log";
		FileWriter fw = new FileWriter(m_XMLFolder_Isolution.getAbsolutePath()+File.separator+logFileName);
		PrintWriter pw = new PrintWriter(fw);
		
		Connection conn = DcmisDB.getConnection();
		String sql = "select * from T_DELETION where STATUS = 1 and TYPE = 1";
		Statement stat = conn.createStatement();
		stat.execute(sql);
		ResultSet result = stat.getResultSet();
		while(result.next()) {
			
			String source = result.getString("source");
			String siteid = result.getString("site_id");
			String forumid = result.getString("forum_id");
			String threadid = result.getString("thread_id");
			int year = result.getInt("year");
			int month = result.getInt("month");
			Date latestExtractionDate1 = result.getDate("LAST_EXTRACTION_DATE1");
			Date latestExtractionDate2 = result.getDate("LAST_EXTRACTION_DATE2");
			
//			String output = "source:"+source;
//			output = output.concat("AND site_id:"+siteid);
//			if(forumid != null)
//				output = output.concat("AND forum_id:"+forumid);
//			if(threadid != null)
//				output = output.concat("AND thread_id:"+threadid);
//			if(year != 0)
//				output = output.concat("AND year:"+year);
//			if(month != 0)
//				output = output.concat("AND month:"+month);
//			if(latestExtractionDate1 != null)
//				output = output.concat("AND lastest_extraction_date1:"+latestExtractionDate1.getTime());
//			if(latestExtractionDate2 != null)
//				output = output.concat(" lastest_extraction_date2:"+latestExtractionDate2.getTime());
			String output = ProcessDeleteItem.generateQueryStr(source, siteid, forumid, threadid, year, month, latestExtractionDate1, latestExtractionDate2);
			
			pw.println(output);
		}
		
		result.close();
		
		pw.close();
		fw.close();
	}

	private void resetDeletionOperation() throws Exception {
		Connection conn = DcmisDB.getConnection();
		String sql = "update T_DELETION set STATUS = '0' WHERE STATUS = '1' AND TYPE = '1'";
		Statement stat = conn.createStatement();
		stat.execute(sql);
	}

	class FileCompareByDate implements Comparator<File> {
		public int compare(File o1, File o2) {
			long time1 = o1.lastModified();
			long time2 = o2.lastModified();
			return (time1 == time2) ? 0 : (time1 > time2 ? 1 : -1);
		}

	}

	public static void main(String[] args) {
		File src = new File("/home/paul/TE_item_remote");
		File dest = new File("/home/paul/TE_item_local");

		ProcessIsolateXML process = new ProcessIsolateXML(src, dest);

		try {
			process.process(null);
		} catch (ProcessIsolationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
