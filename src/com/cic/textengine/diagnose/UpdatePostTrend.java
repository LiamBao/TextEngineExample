package com.cic.textengine.diagnose;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.codec.DecoderException;
import org.apache.log4j.Logger;

import com.cic.textengine.posttrend.PostTrend;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.PartitionUploadChunk;

public class UpdatePostTrend {
	
	private PostTrend posttrend = null;
	private String logfile = null;
	
	private static Logger logger = Logger.getLogger(UpdatePostTrend.class);
	
	public UpdatePostTrend(String logfile)
	{
		posttrend = new PostTrend();
		this.logfile = logfile;
	}
	
	public void update() throws Exception
	{
		LogReader reader = new LogReader(logfile);
		try {
			reader.parseLog();
			ArrayList<PartitionUploadChunk> chunks = reader.getChunks();
			for(int i=0; i<chunks.size(); i++)
			{
				PartitionUploadChunk chunk = chunks.get(i);
				PartitionKey parkey = PartitionKey.decodeStringKey(chunk.getPartitionKey());
				posttrend.addTrend(parkey, chunk.getItemCount());
			}
			posttrend.close();
		} catch (IOException e) {
			logger.error("Error parsing log file:"+logfile+"."+e.getLocalizedMessage());
			throw e;
		} catch (DecoderException e) {
			logger.error("Invalid parkey:"+e.getLocalizedMessage());
			throw e;
		} catch (Exception e) {
			logger.error("Error in add trend to Post trend DB:"+e.getLocalizedMessage());
			throw e;
		}
	}
	
	public static void main(String[] args)
	{
		if(args.length < 1)
		{
			System.out.println("Parameter needed: logfile");
			return ;
		}
		String logfile = args[0];
		UpdatePostTrend update = new UpdatePostTrend(logfile);
		try {
			update.update();
		} catch (Exception e) {
			logger.error("Error update post trend:"+e.getLocalizedMessage());
		}
	}

}
