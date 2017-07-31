package com.cic.textengine.segmentprocessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;

import com.cic.data.ItemMeta;
import com.cic.textengine.type.TEItem;

public class ItemIndexer implements SegmentProcessor {
	private FileSystem fs = null;

	private Configuration conf = null;

	private IndexWriter indexWriter = null;

	private static Logger logger = Logger
			.getLogger(ItemIndexer.class.getName());

	public ItemIndexer(FileSystem fs, Configuration hadoopConf, String indexDir) {
		this.fs = fs;
		this.conf = hadoopConf;
		try {
			Directory fsDirectory = FSDirectory.getDirectory(indexDir, true);
			Analyzer analyzer = new CJKAnalyzer();
			indexWriter = new IndexWriter(fsDirectory, analyzer, true);
		} catch (IOException e) {
			logger.error("Failed to create lucene index");
			e.printStackTrace();
		}
	}

	public void process(String segment) throws IOException {
		Path segmentPath = new Path(segment);
		Path blocks[] = fs.listPaths(segmentPath);
		for (Path block : blocks) {
			if (!fs.isDirectory(block))
				continue;
			MapFile.Reader reader = new MapFile.Reader(fs, block.toString(),
					conf);
			logger.debug("Reading from block " + block);
			LongWritable key = new LongWritable();
			TEItem item = new TEItem();
			while (reader.next(key, item)) {

				Document doc = buildDocument(item);
				logger.debug("Indexing item " + key.toString());
				indexWriter.addDocument(doc);

				key = new LongWritable();
				item = new TEItem();
			}
			reader.close();
		}
	}

	private void addField(Document doc, String fieldName, String fieldValue,
			Store storeTag, Index indexTag) {
		Field luceneField = null;
		if ((fieldName != null) && (fieldName.length() > 0)) {

			luceneField = new Field(fieldName, fieldValue, storeTag, indexTag);

			doc.add(luceneField);
		}
	}

	private Document buildDocument(TEItem item) {
		ItemMeta meta = item.getMeta();
		Document doc = new Document();
		/* add fields within meta data */
		for (String fieldName : ItemMeta.UNTOKENIZED_FIELDS) {
			String fieldValue = meta.getValue(fieldName);
			Index indexTag = Field.Index.UN_TOKENIZED;
			Store storeTag = Field.Store.YES;
			addField(doc, fieldName, fieldValue, storeTag, indexTag);
		}
		for (String fieldName : ItemMeta.TOKENIZED_FIELDS) {
			String fieldValue = meta.getValue(fieldName);
			Index indexTag = Field.Index.TOKENIZED;
			Store storeTag = Field.Store.YES;
			addField(doc, fieldName, fieldValue, storeTag, indexTag);
		}

		/* add Content */
		String fieldName = "content";
		String fieldValue = item.getContent();
		Index indexTag = Field.Index.TOKENIZED;
		Store storeTag = Field.Store.NO;

		addField(doc, fieldName, fieldValue, storeTag, indexTag);

		/* add Subject */
		fieldName = "subject";
		fieldValue = item.getSubject();
		indexTag = Field.Index.TOKENIZED;
		storeTag = Field.Store.YES;

		addField(doc, fieldName, fieldValue, storeTag, indexTag);

		return doc;
	}
	
	public void close() throws IOException{
		indexWriter.optimize();
		indexWriter.close();
	}

	public static void main(String[] args) {
		String indexDir = "/home/paul/develop/IndexRepo";
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			ItemIndexer indexer = new ItemIndexer(fs, conf, indexDir);
			String segment = "TextRepo/100/2006-12/RklEMjEyRklE/0";
			indexer.process(segment);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
