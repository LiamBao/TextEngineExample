package com.cic.textengine.idf;

import java.io.File;

import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;
import com.cic.textengine.idf.impl.TwinIDFEngineImpl;

public class IDFEngineFactory {
	//get an instance of IDFEngien for a particular IDF file
	public static synchronized IDFEngine getNewIDFEngineInstance(File file)
	throws IDFEngineInitException, IDFEngineException{
		IDFEngine engine = null;
		engine = new TwinIDFEngineImpl();
		engine.init(file);
		return engine;
	}
}
