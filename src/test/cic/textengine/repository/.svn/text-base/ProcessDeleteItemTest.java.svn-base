package test.cic.textengine.repository;

import java.io.IOException;

import org.junit.Test;

import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.repository.importer.ProcessDeleteItem;
import com.cic.textengine.repository.importer.exception.ImporterProcessException;

public class ProcessDeleteItemTest {
	@Test
	public void TestDeleteItems()
	{
		try {
			Configurer.config("ItemImporter.properties");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		ProcessDeleteItem delete = new ProcessDeleteItem();
		try {
			delete.process(null);
		} catch (ImporterProcessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
}

}
