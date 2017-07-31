package com.cic.textengine.repository.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cic.textengine.repository.ItemImporter;

public class Configurer {
	private static boolean configured = false;
	
	private static String bloomFilterPath = null;
	private static boolean isBloomFilterOn = true;

	private static String nnDaemonHost = null;
	
	private static int nnDaemonPort = 0;

	private static ArrayList<String> filterNames = null;

	private static String iwmWorkflowHost = null;
	
	private static int iwmWorkflowPort = 0;
	
	private static final Logger logger = Logger.getLogger(Configurer.class);
	
	private static Properties properties;
	public static Properties getProperties() {
		if (properties != null)
			return properties;
		
		throw new RuntimeException("Must call Configurer.config first.");
	}

	public static void config(String cfgFileName) throws IOException {
		properties = new Properties();

		InputStream is = ItemImporter.class.getResourceAsStream("/" +
				 cfgFileName);
		properties.load(is);
		
		bloomFilterPath = properties.getProperty("BloomFilterPath");
		isBloomFilterOn = Boolean.parseBoolean(properties.getProperty("IsBloomOn"));
		nnDaemonHost = properties.getProperty("NNDaemonHost");
		nnDaemonPort = Integer.valueOf(properties.getProperty("NNDaemonPort"));
		String filterStr = properties.getProperty("TEFilters");
		String[] filterArray = filterStr.split(",");
		filterNames = new ArrayList<String>();
		iwmWorkflowHost=properties.getProperty("IWMWorkflowHost");
		iwmWorkflowPort=Integer.valueOf(properties.getProperty("IWMWorkflowPort"));
		for(String name:filterArray)
		{
			filterNames.add(name.trim());
		}
		configured = true;
	}

	public static boolean isConfigured() {
		return configured;
	}

	public static String getSnapShotPath() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static String getNNDaemonHost() {
		return nnDaemonHost;
	}
	
	public static int getNNDaemonPort() {
		return nnDaemonPort;
	}

	public static String getBloomFilterPath() {
		return bloomFilterPath;
	}
	
	public static ArrayList<String> getFilterNames(){
		return filterNames;
	}
	
	public static boolean isBloomFilterOn() {
		return isBloomFilterOn;
	}

	public static void setBloomFilterOn(boolean isBloomFilterOn) {
		Configurer.isBloomFilterOn = isBloomFilterOn;
	}

	public static String getIwmWorkflowHost() {
		return iwmWorkflowHost;
	}

	public static int getIwmWorkflowPort() {
		return iwmWorkflowPort;
	}

}

