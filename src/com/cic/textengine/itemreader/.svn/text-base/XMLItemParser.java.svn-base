package com.cic.textengine.itemreader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import com.cic.data.Item;
import com.cic.data.ItemMeta;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.type.TEItemMeta;

public class XMLItemParser implements ContentHandler, ErrorHandler {
	// ~ Instance fields
	// --------------------------------------------------------

	/** logger */
	private final Logger logger = Logger.getLogger(this.getClass().getName());

	/** SAX XML reader */
	private XMLReader saxReader = null; // xml sax reader

	/** document total counter */
	private int docTotalCounter = 0;

	/** report counter constant */
	private int reportCounterCons = 1000;

	/** report counter */
	private int reportCounter = reportCounterCons;

	/** start of indexing time */
	private long startTime = System.currentTimeMillis();

	/** end of indexing time */
	private long endTime = 0;

	/** field name */
	private String fieldName = "";

	/** field value */
	private StringBuffer fieldValue = new StringBuffer();

	/** current xml doc tree level */
	private byte currentLevel = 0;

	private List<Item> items = new ArrayList<Item>();

	private TEItem item = null;

	private boolean skip = false;

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * create XMLIndexer: xml sax reader and lucene index writer
	 * 
	 * @param xmlReader
	 *            sax based xml reader
	 * @param indexWriter
	 *            lucene index writer
	 */
	public XMLItemParser(XMLReader xmlReader) {
		saxReader = xmlReader;

		// set content handler
		saxReader.setContentHandler(this);

		// set error handler
		saxReader.setErrorHandler(this);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Returns whether lucene index build successful complete
	 * 
	 * @param src
	 *            the xml input source.
	 * 
	 * @return boolean: if build successful complete return true else return
	 *         false.
	 * @throws SAXException 
	 * @throws IOException 
	*/
	public List<Item> parse(InputSource src) throws IOException, SAXException {
		if (src == null)
			throw new SAXException("Invalid input source for parsing.");
		saxReader.parse(src);
		endTime = System.currentTimeMillis();
		logger.debug(docTotalCounter + " rows added\tTotal time Use:"
				+ ((endTime - startTime) / 1000) + " second");
		List<Item> result = items;
		items = new ArrayList<Item>();
		return result;
	}

	/**
	 * Implementation of org.xml.sax.ContentHandler.
	 * 
	 * @param locator
	 *            document locator
	 */
	public void setDocumentLocator(Locator locator) {
	}

	/**
	 * init counter
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void startDocument() throws SAXException {
		// init Counter
		docTotalCounter = 0;

		// start at root level
		currentLevel = 0;
	}

	/**
	 * end of sax process
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void endDocument() throws SAXException {
	}

	/**
	 * start of prefix mapping
	 * 
	 * @param prefix
	 *            prefixe
	 * @param uri
	 *            uri
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void startPrefixMapping(String prefix, String uri)
			throws SAXException {
	}

	/**
	 * end of prefix mapping
	 * 
	 * @param prefix
	 *            prefix
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void endPrefixMapping(String prefix) throws SAXException {
	}

	/**
	 * start xml element: switch node level and read element to create lucene
	 * document
	 * 
	 * @param namespaceURI
	 *            namespace
	 * @param localName
	 *            local name
	 * @param qName
	 *            qaulified name
	 * @param atts
	 *            attributes
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void startElement(String namespaceURI, String localName,
			String qName, Attributes atts) throws SAXException {
		// to sub level
		currentLevel++;

		switch (currentLevel) {
		case 1: // table level
			break;

		case 2: // record level
			item = new TEItem();
			ItemMeta meta = new TEItemMeta();
			item.setMeta(meta);
			skip = false;
			break;

		case 3: // field level

			try {
				fieldName = localName.trim();

				// default values
				fieldValue = new StringBuffer();

			} catch (Exception e) {
				logger.error(e.toString());
			}

			break;
		}
	}

	/**
	 * end element handler: switch node level to write to lucene index
	 * 
	 * @param namespaceURI
	 *            uri
	 * @param localName
	 *            local name
	 * @param qName
	 *            qualified name
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void endElement(String namespaceURI, String localName, String qName)
			throws SAXException {

		switch (currentLevel) {
		case 1: // table level
			break;

		case 2: // record level

			try {
				if (skip)
					return;

				items.add(item);
				docTotalCounter++;
				reportCounter--;

				if (reportCounter == 0) {
					// show status;
					endTime = System.currentTimeMillis();
					logger.info(docTotalCounter + " rows added\ttime Use:"
							+ ((endTime - startTime) / 1000) + " second");

					// reset reportCounter
					reportCounter = reportCounterCons;
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}

			break;

		case 3: // field level
			if ((fieldName != null) && (fieldName.length() > 0)) {
				try {
					item.setValue(fieldName, fieldValue.toString().trim());
				} catch (Exception ex) {
					logger.error(String.format(
							"Failed to parse %s for field %s", fieldValue,
							fieldName));
					logger.error(ex.getLocalizedMessage());
					skip = true;
				}
			}
			break;
		}

		// back to up level
		currentLevel--;
	}

	/**
	 * append char array
	 * 
	 * @param ch
	 *            current content
	 * @param start
	 *            start offset
	 * @param length
	 *            content length
	 * 
	 * @throws SAXException
	 *             SAX parse exception
	 */
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		// read field value
		if (currentLevel == 3) {
			/*
			 * NOTICE: if use: fieldValue = new String(ch, start, length) may
			 * cause xml data value broken during saxReader reaches buffer end
			 * for example: <SomeTag>my content</SomeTag> privous sax buffer
			 * reached here---^ after next buffer read will invoke another
			 * characters() event so the fieldValue will return broken value
			 * "ntent" only
			 */
			fieldValue.append(ch, start, length);
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param ch
	 *            DOCUMENT ME!
	 * @param start
	 *            DOCUMENT ME!
	 * @param length
	 *            DOCUMENT ME!
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void ignorableWhitespace(char[] ch, int start, int length)
			throws SAXException {
	}

	/**
	 * processing instruction
	 * 
	 * @param target
	 *            doc
	 * @param data
	 *            data
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void processingInstruction(String target, String data)
			throws SAXException {
	}

	/**
	 * skip entitiy
	 * 
	 * @param name
	 *            name
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void skippedEntity(String name) throws SAXException {
	}

	/**
	 * Implementation of org.xml.sax.ErrorHandler.
	 * 
	 * @param e
	 *            sax parse exception
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void warning(SAXParseException e) throws SAXException {
		logger.error("  EVENT: warning " + e.getMessage() + ' '
				+ e.getSystemId() + ' ' + e.getLineNumber() + ' '
				+ e.getColumnNumber());
	}

	/**
	 * error log
	 * 
	 * @param e
	 *            sax parse exception
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void error(SAXParseException e) throws SAXException {
		logger.error("  EVENT: error " + e.getMessage() + ' ' + e.getSystemId()
				+ ' ' + e.getLineNumber() + ' ' + e.getColumnNumber());
	}

	/**
	 * fatal error log
	 * 
	 * @param e
	 *            sax exception
	 * 
	 * @throws SAXException
	 *             sax exceptions
	 */
	public void fatalError(SAXParseException e) throws SAXException {
		logger.error("  EVENT: fatal error " + e.getMessage() + ' '
				+ e.getSystemId() + ' ' + e.getLineNumber() + ' '
				+ e.getColumnNumber());
	}
	
	public void close(){
		saxReader = null;
	}
}
