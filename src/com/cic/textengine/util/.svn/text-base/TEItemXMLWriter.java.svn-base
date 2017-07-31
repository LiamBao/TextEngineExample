package com.cic.textengine.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import com.cic.data.Item;
import com.cic.textengine.config.Configurer;
import com.cic.textengine.idgenarator.IDGenerator;
import com.cic.textengine.itemreader.ItemReader;
import com.cic.textengine.itemreader.XMLItemReader;
import com.cic.textengine.type.TEItem;

/**
 * TEItemXMLWriter is to write an TEItem instance to an UTF-8 encoded XML file
 * based on predefined scheme.
 * 
 * @author textd
 * 
 */
public class TEItemXMLWriter {
	private final static String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

	private final static String XML_TE = "TE";

	private final static String XML_TEITEM = "TEItem";

	private final static String[] FIELDS = new String[] { "SiteID", "ForumID",
			"ThreadID", "Poster", "DateOfPost", "TopicPost", "ItemUrl",
			"SiteName", "ForumName", "ForumUrl", "FirstExtractionDate",
			"LatestExtractionDate", "PosterID", "PosterUrl", "ItemType",
			"Source", "KeywordGroup", "Keyword", "Subject", "Content" };

	private final static String[] ITEM_FIELDS = new String[] { "content" };

	private BufferedWriter bufWriter = null;

	public String escape(String content) {
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < content.length(); i++) {
			char c = content.charAt(i);
			if (c == '<')
				buffer.append("&lt;");
			else if (c == '>')
				buffer.append("&gt;");
			else if (c == '&')
				buffer.append("&amp;");
			else if (c == '"')
				buffer.append("&quot;");
			else if (c == '\'')
				buffer.append("&apos;");
			else
				buffer.append(c);
		}
		return buffer.toString();
	}

	/**
	 * To initialize the TEItemXMLWriter by specifying the file name, If
	 * successful, the file will be created, and header information will be
	 * written.
	 * 
	 * @param fileName
	 *            The fileName of the XML file.
	 * @throws IOException
	 *             When fail to create the xml file or write the header
	 *             information.
	 */
	public TEItemXMLWriter(String fileName) throws IOException {
		this.bufWriter = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(fileName), "UTF8"));
		bufWriter.write(XML_HEADER);
		bufWriter.newLine();
		bufWriter.write("<" + XML_TE + ">");
		bufWriter.newLine();
	}

	/**
	 * Write a single instance of TEItem into XML.
	 * 
	 * @param item
	 *            The TEItem instance to write.
	 * @throws IOException
	 *             When any of the item's fields failed to be written.
	 * 
	 */
	public void write(TEItem item) throws IOException {
		bufWriter.write("<" + XML_TEITEM + ">");
		bufWriter.newLine();
		for (String field : FIELDS) {
			bufWriter.write("<" + field + ">");
			bufWriter.newLine();
			String value = item.getValue(field);
			value = value != null ? value : "";
			value = escape(value);
			bufWriter.write(value);
			bufWriter.newLine();
			bufWriter.write("</" + field + ">");
			bufWriter.newLine();
		}
		bufWriter.write("</" + XML_TEITEM + ">");
		bufWriter.newLine();
	}

	/**
	 * Close the TEItenXMLWriter, during the process the ending information file
	 * will be closed
	 * 
	 * @throws IOException
	 *             Fail to write the ending information or close the file.
	 */
	public void close() throws IOException {
		bufWriter.write("</" + XML_TE + ">");
		bufWriter.newLine();
		bufWriter.close();
	}

	public static void main(String[] args) {
		ItemReader reader = null;
		Configurer.config("/home/textd/develop/TextEngine/conf/config.txt");
		IDGenerator.getInstance(100);
		try {
			reader = new XMLItemReader("/home/textd/develop/items/testin");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String output = ("/home/textd/develop/items/testout/out.xml");
		try {
			TEItemXMLWriter writer = new TEItemXMLWriter(output);
			while (reader.next()) {
				Item item = reader.getItem();
				writer.write((TEItem) item);
			}
			writer.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}