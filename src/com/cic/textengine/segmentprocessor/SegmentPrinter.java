package com.cic.textengine.segmentprocessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.log4j.Logger;
import com.cic.data.ItemMeta;
import com.cic.textengine.type.TEItem;

public class SegmentPrinter implements SegmentProcessor {
	private FileSystem fs = null;

	private Configuration conf = null;

	private static Logger logger = Logger
			.getLogger(SegmentProcessor.class.getName());
	
	private String fields = null;

	public SegmentPrinter(FileSystem fs, Configuration hadoopConf) {
		this.fs = fs;
		this.conf = hadoopConf;
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
				print(item);
				key = new LongWritable();
				item = new TEItem();
			}
			reader.close();
		}		
	}
	public void setFields(String fields){
		this.fields = fields.trim();
	}
	
	private void print(TEItem item) {
		ItemMeta meta = item.getMeta();
		/* add fields within meta data */
		System.out.println("<Item>");
		String[] fieldNames = fields.split(",");
		for (String fieldName : fieldNames) {
			String fieldValue = meta.getValue(fieldName);
			System.out.println(String.format("<%s>", fieldName));
			System.out.println(String.format("%s", fieldValue));
			System.out.println(String.format("</%s>", fieldName));
		}
		
		System.out.println("</Item>");
	}
	
	public static void main(String[] args){
		//String segment = "/home/paul/develop/workspace/TextEngine/bin/TextRepo/1/2006-12/Q2xhcml0aW5fU2VhcmNo/0/";
		String segment = "/home/paul/develop/testsegment";
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			SegmentPrinter printer = new SegmentPrinter(fs, conf);
			printer.process(segment);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}
}
