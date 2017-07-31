package com.cic.textengine.repository;

import java.io.IOException;

import com.cic.textengine.repository.datanode.client.exception.RemoteTEItemWriterException;
import com.cic.textengine.type.TEItem;

public interface TEWriter {
	public void writeTEItem(TEItem item) throws IOException;
	
	public void flush() throws IOException;

	public void close() throws IOException, RemoteTEItemWriterException;
}
