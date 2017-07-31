package test.cic.textengine.idf;

import java.io.File;

import org.junit.Test;

import com.cic.textengine.idf.IDFEnginePool;
import com.cic.textengine.idf.exception.IDFEngineException;
import com.cic.textengine.idf.exception.IDFEngineInitException;

public class IDFEnginePoolTest {

	@Test
	public void testGetIDFEngineInstance() {
		IDFEnginePool p = IDFEnginePool.getInstance();
		p.setPoolSize(10);
		
		for (int i = 0;i<100;i++){
			File file = new File("d:\\" + i + ".idf");
			try {
				p.getIDFEngineInstance(file);
			} catch (IDFEngineInitException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IDFEngineException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
