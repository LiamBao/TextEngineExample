package test.cic.textengine.type;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import com.cic.textengine.type.TEItem;

public class TEItemTest extends TestCase {
	TEItem testItem = null;

	protected void setUp() throws Exception {
	}

	public void testSerialize() {
		try {
			FileOutputStream fos = new FileOutputStream("/home/CICDATA/denis.yu/item.dat");
			fos.close();
		} catch (FileNotFoundException e) {
			fail("Exception found.");
			e.printStackTrace();
		} catch (IOException e) {
			fail("Exception found.");
			e.printStackTrace();
		}
		
	}

	public void testDeserailize() {
		try {
			FileInputStream fis = new FileInputStream("/home/CICDATA/denis.yu/item.dat");
		} catch (FileNotFoundException e) {
			fail("Exception found.");
			e.printStackTrace();
		} catch (IOException e) {
			fail("Exception found.");
			e.printStackTrace();
		}		
	}
}
