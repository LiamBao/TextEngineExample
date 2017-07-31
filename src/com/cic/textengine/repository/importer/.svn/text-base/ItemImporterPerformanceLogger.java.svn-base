package com.cic.textengine.repository.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public class ItemImporterPerformanceLogger {

	String FILENAME_PERFORMANCE_LOG = "ItemImporterPerformance.log";
	SimpleDateFormat m_dtFormater = new SimpleDateFormat(
			"yyyy.MM.dd HH:mm:ss.SSS");

	String NUMBER_PARSED_ITEM = "|NUMBER_OF_PARSED_ITEM";
	String NUMBER_DUPLICATED_ITEM = "|NUMBER_OF_DUPLICATED_ITEM";
	String TIME_PARSE_ITEM = "|TIME_OF_PARSE_ITEM";

	String NUMBER_UPLOAD_ITEM = "|NUMBER_OF_UPLOADED_ITEM";
	String TIME_UPLOAD_ITEM = "|TIME_OF_UPLOAD_ITEM";

	String NUMBER_INDEX_ITEM = "|NUMBER_OF_INDEXED_ITEM";
	String TIME_INDEX_ITEM = "|TIME_OF_INDEX_ITEM";

	String NUMBER_DELETE_ITEM = "|NUMBER_OF_DELETE_ITEM";
	String TIME_DELETE_ITEM = "|TIME_OF_DELETE_ITEM";

	String NUMBER_CONSOLIDATE_ITEM = "|NUMBER_OF_CONSOLIDATE_ITEM";
	String TIME_CONSOLIDATE_ITEM = "|TIME_OF_CONSOLIDATE_ITEM";

	String TIME_TOTAL_SESSION = "|TIME_OF_WHOLE_SESSION";

	// PrintWriter m_printWriter = null;

	long m_itemCount_XMLParser = 0;
	long m_itemCount_Duplicated_XMLParser = 0;
	long m_totalTime_XMLParser = 0;

	long m_itemCount_ItemUploader = 0;
	long m_totalTime_ItemUploader = 0;

	long m_itemCount_ItemIndex = 0;
	long m_totalTime_ItemIndex = 0;

	long m_itemCount_ItemDelete = 0;
	long m_totalTime_ItemDelete = 0;

	long m_itemCount_ItemSolidify = 0;
	long m_totalTime_ItemSolidify = 0;

	long m_sessionStartTime = 0;
	long m_sessionTime = 0;

	public ItemImporterPerformanceLogger() throws IOException {
		loadPreviousState();
	}

	public String getStrTimestamp() {
		Calendar c = Calendar.getInstance();
		return m_dtFormater.format(c.getTime());
	}

	public void startSession() {

		m_sessionStartTime = System.currentTimeMillis();

	}

	public synchronized void logXMLParserPerformance(long itemCount,
			long duplicatedItemCount, long startDT) throws IOException {
		long dt = System.currentTimeMillis();
		long time = dt - startDT;
		if (itemCount == 0)
			return;

		m_itemCount_XMLParser += itemCount;
		m_itemCount_Duplicated_XMLParser += duplicatedItemCount;
		m_totalTime_XMLParser += time;

		writeLog();

	}

	public synchronized void logItemUploaderPerformance(long itemCount,
			long startDT) throws IOException {
		long dt = System.currentTimeMillis();
		long time = dt - startDT;
		if (itemCount == 0)
			return;

		m_itemCount_ItemUploader += itemCount;
		m_totalTime_ItemUploader += time;

		writeLog();
	}

	public synchronized void logItemIndexPerformance(long itemCount,
			long startDT) throws IOException {
		long dt = System.currentTimeMillis();
		long time = dt - startDT;
		if (itemCount == 0)
			return;

		m_itemCount_ItemIndex += itemCount;
		m_totalTime_ItemIndex += time;

		writeLog();
	}

	public synchronized void logItemDeletePerformance(long itemCount,
			long startDT) throws IOException {
		long dt = System.currentTimeMillis();
		long time = dt - startDT;
		if (itemCount == 0)
			return;

		m_itemCount_ItemDelete += itemCount;
		m_totalTime_ItemDelete += time;

		writeLog();
	}

	public synchronized void logItemSolidifyPerformance(long itemCount,
			long startDT) throws IOException {
		long dt = System.currentTimeMillis();
		long time = dt - startDT;
		if (itemCount == 0)
			return;

		m_itemCount_ItemSolidify += itemCount;
		m_totalTime_ItemSolidify += time;

		writeLog();
	}

	public void closeSession() {

		m_sessionTime += (System.currentTimeMillis() - m_sessionStartTime);
	}

	void loadPreviousState() throws IOException {
		File logFile = new File(FILENAME_PERFORMANCE_LOG);
		if(!logFile.exists())
		{
			logFile.createNewFile();
			writeLog();
			return;
		}
		FileInputStream is = new FileInputStream(logFile);
		Properties prop = new Properties();
		prop.load(is);
		is.close();
		String s_itemCount_XMLParser = prop.getProperty(NUMBER_PARSED_ITEM);
		String s_itemCount_Duplicated_XMLParser = prop
				.getProperty(NUMBER_DUPLICATED_ITEM);
		String s_totalTime_XMLParser = prop.getProperty(TIME_PARSE_ITEM);

		String s_itemCount_ItemUploader = prop.getProperty(NUMBER_UPLOAD_ITEM);
		String s_totalTime_ItemUploader = prop.getProperty(TIME_UPLOAD_ITEM);

		String s_itemCount_ItemIndex = prop.getProperty(NUMBER_INDEX_ITEM);
		String s_totalTime_ItemIndex = prop.getProperty(TIME_INDEX_ITEM);

		String s_itemCount_ItemDelete = prop.getProperty(NUMBER_DELETE_ITEM);
		String s_totalTime_ItemDelete = prop.getProperty(TIME_DELETE_ITEM);

		String s_itemCount_ItemSolidify = prop
				.getProperty(NUMBER_CONSOLIDATE_ITEM);
		String s_totalTime_ItemSolidify = prop.getProperty(TIME_CONSOLIDATE_ITEM);

		String s_sessionTime = prop.getProperty(TIME_TOTAL_SESSION);

		m_itemCount_XMLParser = Long.valueOf(s_itemCount_XMLParser.substring(0,
				s_itemCount_XMLParser.lastIndexOf("c")));
		m_itemCount_Duplicated_XMLParser = Long
				.valueOf(s_itemCount_Duplicated_XMLParser.substring(0,
						s_itemCount_Duplicated_XMLParser.lastIndexOf("c")));
		m_totalTime_XMLParser = Long.valueOf(s_totalTime_XMLParser.substring(0,
				s_totalTime_XMLParser.lastIndexOf("c")));

		m_itemCount_ItemUploader = Long.valueOf(s_itemCount_ItemUploader
				.substring(0, s_itemCount_ItemUploader.lastIndexOf("c")));
		m_totalTime_ItemUploader = Long.valueOf(s_totalTime_ItemUploader
				.substring(0, s_totalTime_ItemUploader.lastIndexOf("c")));

		m_itemCount_ItemIndex = Long.valueOf(s_itemCount_ItemIndex.substring(0,
				s_itemCount_ItemIndex.lastIndexOf("c")));
		m_totalTime_ItemIndex = Long.valueOf(s_totalTime_ItemIndex.substring(0,
				s_totalTime_ItemIndex.lastIndexOf("c")));

		m_itemCount_ItemDelete = Long.valueOf(s_itemCount_ItemDelete.substring(
				0, s_itemCount_ItemDelete.lastIndexOf("c")));
		m_totalTime_ItemDelete = Long.valueOf(s_totalTime_ItemDelete.substring(
				0, s_totalTime_ItemDelete.lastIndexOf("c")));

		m_itemCount_ItemSolidify = Long.valueOf(s_itemCount_ItemSolidify
				.substring(0, s_itemCount_ItemSolidify.lastIndexOf("c")));
		m_totalTime_ItemSolidify = Long.valueOf(s_totalTime_ItemSolidify
				.substring(0, s_totalTime_ItemSolidify.lastIndexOf("c")));

		m_sessionTime = Long.valueOf(s_sessionTime.substring(0, s_sessionTime
				.lastIndexOf("c")));
	}

	void writeLog() throws IOException {
		// add "c" to tell Nagios to calculate the incremental
		Properties prop = new Properties();
		prop.setProperty(NUMBER_PARSED_ITEM, Long
				.toString(m_itemCount_XMLParser)
				+ "c");
		prop.setProperty(NUMBER_DUPLICATED_ITEM, Long
				.toString(m_itemCount_Duplicated_XMLParser)
				+ "c");
		prop.setProperty(TIME_PARSE_ITEM, Long.toString(m_totalTime_XMLParser)
				+ "c");

		prop.setProperty(NUMBER_UPLOAD_ITEM, Long
				.toString(m_itemCount_ItemUploader)
				+ "c");
		prop.setProperty(TIME_UPLOAD_ITEM, Long
				.toString(m_totalTime_ItemUploader)
				+ "c");

		prop.setProperty(NUMBER_INDEX_ITEM, Long
				.toString(m_itemCount_ItemIndex)
				+ "c");
		prop.setProperty(TIME_INDEX_ITEM, Long.toString(m_totalTime_ItemIndex)
				+ "c");

		prop.setProperty(NUMBER_DELETE_ITEM, Long
				.toString(m_itemCount_ItemDelete)
				+ "c");
		prop.setProperty(TIME_DELETE_ITEM, Long
				.toString(m_totalTime_ItemDelete)
				+ "c");

		prop.setProperty(NUMBER_CONSOLIDATE_ITEM, Long
				.toString(m_itemCount_ItemSolidify)
				+ "c");
		prop.setProperty(TIME_CONSOLIDATE_ITEM, Long
				.toString(m_totalTime_ItemSolidify)
				+ "c");

		prop
				.setProperty(TIME_TOTAL_SESSION, Long.toString(m_sessionTime)
						+ "c");

		FileOutputStream fos = new FileOutputStream(FILENAME_PERFORMANCE_LOG,
				false);
		prop.store(fos, "Item Importer Performance Log.");
		fos.close();

	}

	public static void main(String args[]) {
		try {
			ItemImporterPerformanceLogger per = new ItemImporterPerformanceLogger();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
