package com.cic.textengine.itemreader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;
import com.cic.data.Item;
import com.cic.textengine.type.exception.XMLParsingException;

public class XMLItemReader implements ItemReader {

	private XMLItemParser parser = null;
	private XMLThreadItemParser threadParser = null;

	private File[] files = null;

	private int elementIndex = 0;

	private int fileIndex = 0;

	private Item currentItem = null;

	private static Logger logger = Logger.getLogger(XMLItemReader.class.getName());

	private List<Item> items = null;

	public XMLItemReader(String path) throws Exception {
		File dir = new File(path);
		if (!dir.exists()) {
			return;
		}
		
		FilenameFilter xmlFilter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				name = name.toLowerCase();
				if (name.endsWith(".xml") || name.endsWith(".gz")) {
					return true;
				} else {
					return false;
				}
			}
		};
		if(dir.isFile()){
			if(xmlFilter.accept(dir,dir.getAbsolutePath())){
				files = new File[]{dir};
			}else{
				return;
			}
		}else{
			files = dir.listFiles(xmlFilter);
			Arrays.sort(files);
		}

		String driverName = System.getProperty("org.xml.sax.driver", "org.apache.xerces.parsers.SAXParser");
		XMLReader xmlReader = XMLReaderFactory.createXMLReader(driverName);
		parser = new XMLItemParser(xmlReader);
		XMLReader xmlReaderThread = XMLReaderFactory.createXMLReader(driverName);
		threadParser = new XMLThreadItemParser(xmlReaderThread);
	}

	public Item getItem() {
		return currentItem;
	}

	private boolean hasNextWithinFile() {
		int itemSize = items != null ? items.size() : 0;
		if (items != null && elementIndex < itemSize) {
			/* if there are still elements from last batch */
			Item item = null;
			while (item == null && elementIndex < itemSize) {
				item = items.get(elementIndex);
				elementIndex++;
			}
			currentItem = item;
			return true;
		}
		return false;
	}

	public boolean next() throws XMLParsingException {
		/* if there are still items from the last parsed xml */
		if (hasNextWithinFile())
			return true;

		/* if there are no items left and no more files */
		if (fileIndex == files.length)
			return false;

		/* if there are item files remained to parse */
		items = null;
		elementIndex = 0;
		while (items == null && fileIndex < files.length) {
			String itemPath = files[fileIndex].getAbsolutePath();
			try {
				items = parseItem(itemPath, parser);
			} catch (XMLParsingException e) {
				logger.error("Fail to parse item in file: " + itemPath);
				e.setItemPath(itemPath);
				throw e;
			}

			fileIndex++;
		}
		return next();
	}

	public List<Item> parseItem(String itemPath, XMLItemParser parser) throws XMLParsingException {
		String[] codecNames = new String[] { "UTF-8", "UTF-16LE", "GBK", "UTF-16", "ISO-8859-1", "UTF-16BE" };
		XMLInvalidCharFilter filter = null;
		boolean isNeedParse = true;
		XMLParsingException parsingException = null;
		for (int i = 0; isNeedParse && i < codecNames.length; ++i) {
			try {
				if (itemPath.endsWith("xml"))
					filter = new XMLInvalidCharFilter(new FileInputStream(itemPath), codecNames[i]);

				if (itemPath.endsWith("gz"))
					filter = new XMLInvalidCharFilter(new GZIPInputStream(new FileInputStream(itemPath)),
					                                  codecNames[i]);

				if (filter == null) {
					throw new IOException("No item files found.");
				}

			} catch (FileNotFoundException e) {
				throw new XMLParsingException(e);
			} catch (UnsupportedEncodingException e) {
				parsingException = new XMLParsingException(e);
				break;
			} catch (IOException e) {
				parsingException = new XMLParsingException(e);
				break;
			}

			InputSource input = new InputSource(filter);
			if (input == null)
				throw new XMLParsingException("Fail to get input source for parsing");

			if (parser == null)
				throw new XMLParsingException("Invalid parser");

			try {
				boolean isThreadXml = (new File(itemPath)).getName().startsWith("TEItem_FFT_");
				if (isThreadXml)
					items = threadParser.parse(input);
				else
					items = parser.parse(input);
				isNeedParse = false;
			} catch (IOException e) {
				parsingException = new XMLParsingException(e);
			} catch (SAXException e) {
				parsingException = new XMLParsingException(e);
			} finally {
				try {
					filter.close();
				} catch (IOException e) {
					logger.error("Error closing " + itemPath);
				}
			}
		}
		if(isNeedParse && parsingException != null){
			throw parsingException;
		}
		return items;
	}

	/**
	 * @deprecated
	 */
	public List<Item> parseItem_old(String itemPath, XMLItemParser parser) throws XMLParsingException {
		XMLInvalidCharFilter filter = null;
		try {
			if (itemPath.endsWith("xml"))
				filter = new XMLInvalidCharFilter(new FileInputStream(itemPath), "UTF-8");

			if (itemPath.endsWith("gz"))
				filter = new XMLInvalidCharFilter(new GZIPInputStream(new FileInputStream(itemPath)), "UTF-8");

			if (filter == null) {
				throw new IOException("No item files found.");
			}

		} catch (FileNotFoundException e) {
			throw new XMLParsingException(e);
		} catch (UnsupportedEncodingException e) {
			throw new XMLParsingException(e);
		} catch (IOException e) {
			throw new XMLParsingException(e);
		}

		InputSource input = new InputSource(filter);
		if (input == null)
			throw new XMLParsingException("Fail to get input source for parsing");

		if (parser == null)
			throw new XMLParsingException("Invalid parser");

		try {
			boolean isThreadXml = (new File(itemPath)).getName().startsWith("TEItem_FFT_");
			if (isThreadXml)
				items = threadParser.parse(input);
			else
				items = parser.parse(input);
		} catch (IOException e) {
			throw new XMLParsingException(e);
		} catch (SAXException e) {
			throw new XMLParsingException(e);
		} finally {
			try {
				filter.close();
			} catch (IOException e) {
				logger.error("Error closing " + itemPath);
			}
		}
		return items;
	}

	public void close() {
		if (items != null) {
			items = null;
		}

		if (parser != null)
			parser.close();
		if (threadParser != null)
			threadParser.close();
	}

	public boolean isGenEnd(int cuFileIndex, int cuItemIndex) {
		List<Item> items2 = null;
		try {
			items2 = this.parseItem(this.files[this.files.length - 1].getAbsolutePath(), this.parser);
		} catch (XMLParsingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(this.files.length + "/========================" + items2.size());
		System.out.println(cuFileIndex + "/========================" + cuItemIndex);
		return (this.files.length == cuFileIndex && items2.size() == cuItemIndex);
	}

	public static void main(String[] args) {

		String file = args[0];
		try {
			XMLItemReader reader = new XMLItemReader(file);
			while (reader.next()) {
				int amdCount = 0;
				int msCount = 0;
				int nvCount = 0;
				int ibmCount = 0;
				int dellCount = 0;
				int hpCount = 0;
				int lenovoCount = 0;
				int appCount = 0;

				Item item = reader.getItem();
				String str = item.getSubject() + item.getContent();
				if (str.contains("AMD") || str.contains("³¬Î¢"))
					amdCount++;
				if (str.contains("Microsoft") || str.contains("Î¢Èí"))
					msCount++;
				if (str.contains("NVIDIA") || str.contains("NV") || str.contains("Ó¢Î°´ï"))
					nvCount++;
				if (str.contains("IBM"))
					ibmCount++;
				if (str.contains("Dell") || str.contains("´÷¶û"))
					dellCount++;
				if (str.contains("HP") || str.contains("»ÝÆÕ"))
					hpCount++;
				if (str.contains("Lenovo") || str.contains("ÁªÏë"))
					lenovoCount++;
				if (str.contains("Apple") || str.contains("Æ»¹û") || str.contains("Mac"))
					appCount++;

				System.out.println(amdCount);
				System.out.println(msCount);
				System.out.println(nvCount);
				System.out.println(ibmCount);
				System.out.println(dellCount);
				System.out.println(hpCount);
				System.out.println(lenovoCount);
				System.out.println(appCount);

			}
			// System.out.println(reader.isGenEnd(1, 1));
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	public int getElementIndex() {
		return elementIndex;
	}

	public void setElementIndex(int elementIndex) {
		this.elementIndex = elementIndex;
	}

	public int getFileIndex() {
		return fileIndex;
	}

	public void setFileIndex(int fileIndex) {
		this.fileIndex = fileIndex;
	}
}
