package test.cic.textengine.datadelivery;

import org.junit.Test;

import com.cic.textengine.datadelivery.DataSolidify;

public class TestDataSolidify {

	@Test
	public void testDataSolidifyFromTR()
	{
		String nnHost = "192.168.2.2";
		int nnPort = 6869;
		DataSolidify ds = new DataSolidify(nnHost, nnPort);
		try {
			ds.solidify(2008, 6, "FF", "349", "FIDcorolla.iFID");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
