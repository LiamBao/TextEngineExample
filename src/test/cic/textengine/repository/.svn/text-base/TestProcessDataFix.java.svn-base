package test.cic.textengine.repository;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import com.cic.textengine.repository.importer.ItemImporterPerformanceLogger;
import com.cic.textengine.repository.importer.ProcessDataConsolidate;
import com.cic.textengine.repository.importer.exception.ImporterProcessException;

public class TestProcessDataFix {

	@Test
	public void testDataFix()
	{
		ProcessDataConsolidate datafix = new ProcessDataConsolidate(null);
		ItemImporterPerformanceLogger perfLogger = null;
		try {
			perfLogger = new ItemImporterPerformanceLogger();
			datafix.process(perfLogger);
		} catch (IOException e3) {
			System.out.println("Error initing performance logger, halt the process.");
		} catch (ImporterProcessException e) {
			e.printStackTrace();
		}

	}
}
