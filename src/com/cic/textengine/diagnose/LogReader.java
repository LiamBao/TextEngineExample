package com.cic.textengine.diagnose;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.cic.textengine.type.PartitionUploadChunk;

public class LogReader {
	
	private String logFilePath = null;
	private File log = null;
	private ArrayList<PartitionUploadChunk> chunks = null;
	
	private static Logger logger = Logger.getLogger(LogReader.class);
	
	public LogReader(String log)
	{
		this.logFilePath = log;
		this.log = new File(this.logFilePath);
		this.chunks = new ArrayList<PartitionUploadChunk>();
	}
	public ArrayList<PartitionUploadChunk> getChunks()
	{
		return chunks;
	}
	
	public void parseLog() throws IOException
	{
		FileReader logreader = new FileReader(log);
		BufferedReader br = new BufferedReader(logreader);
		String line = null;
		while((line = br.readLine())!=null)
		{
			String[] splitline = line.split(",");
			String parkey = splitline[0].split(":")[1];
			String startid = splitline[1].split(":")[1];
			String itemcount = splitline[2].split(":")[1].split("]")[0];
//			System.out.println(line);
			try {
				PartitionUploadChunk chunk = new PartitionUploadChunk(parkey, Long.parseLong(startid), Integer.parseInt(itemcount));
				chunks.add(chunk);
				logger.info(String.format("Chunk read. [ParKey:%s, StartItemID:%s, ItemCount:%s]", parkey, startid, itemcount));
			} catch (NumberFormatException e)
			{
				logger.error("Wrong partition chunk log:"+line);
			}
		}
	}
	
	public static void main(String[] args)
	{
		LogReader reader = new LogReader("/home/joe.sun/trend");
		try {
			reader.parseLog();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
