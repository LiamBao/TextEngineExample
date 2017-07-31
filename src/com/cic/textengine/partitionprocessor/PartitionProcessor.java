package com.cic.textengine.partitionprocessor;

import java.io.IOException;

public interface PartitionProcessor {

	void process(String partition) throws IOException;

}
