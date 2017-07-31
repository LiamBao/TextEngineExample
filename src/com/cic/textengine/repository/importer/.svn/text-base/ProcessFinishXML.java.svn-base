package com.cic.textengine.repository.importer;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.commons.io.FileUtils;

import com.cic.textengine.repository.importer.exception.ProcessFinishXMLException;


public class ProcessFinishXML implements ImporterProcess {
	Logger m_logger = Logger.getLogger(ProcessFinishXML.class);
	
	File m_XMLFolder_Isolution = null;
	File m_XMLFolder_Finished = null;
	
	public ProcessFinishXML(File isolationPath, File finishPath){
		m_XMLFolder_Isolution = isolationPath;
		m_XMLFolder_Finished = finishPath;
	}
	
	public void process(ItemImporterPerformanceLogger perfLogger) throws ProcessFinishXMLException {
		m_logger
				.info("Moving XML from isolation area to finished folder for backing up...");

		File[] files = m_XMLFolder_Isolution.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				name = name.toLowerCase();
				if (name.endsWith(".xml") || name.endsWith(".gz")  || name.endsWith(".log")) {
					return true;
				} else {
					return false;
				}
			}
		});

		if (files.length<=0)
			return;
		
		File src, dest;
		dest = new File(this.m_XMLFolder_Finished.getAbsolutePath() + File.separator + System.currentTimeMillis());
		dest.mkdirs();
		String dest_parent = dest.getAbsolutePath();
		
		for (int i = 0; i < files.length; i++) {
			src = files[i];
			dest = new File(dest_parent
					+ File.separator + src.getName());
//			src.renameTo(dest);
			try {
				FileUtils.moveFile(src, dest);
			} catch (IOException e) {
				m_logger.error(String.format("Fail to move file from %s to %s.", src, dest));
				throw new ProcessFinishXMLException(e);
			}
		}
	}
}
