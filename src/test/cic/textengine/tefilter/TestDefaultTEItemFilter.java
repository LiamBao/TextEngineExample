package test.cic.textengine.tefilter;

import java.util.ArrayList;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.TEItemEnumerator;
import com.cic.textengine.client.exception.TEItemEnumeratorException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.tefilter.exception.TEItemFilterException;
import com.cic.textengine.tefilter.impl.DefaultFilter;
import com.cic.textengine.type.TEItem;

import junit.framework.TestCase;

public class TestDefaultTEItemFilter extends TestCase{

	public void testFilterOutput()
	{
		DefaultFilter filter = new DefaultFilter();
		try {
			filter.init();
		} catch (TEItemFilterException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ArrayList<String> itemKeyList = new ArrayList<String>();
		TEClient client = new TEClient("192.168.2.2", 6869);
		String parKey = (new PartitionKey(2008, 10, "FF101", "FID185FID")).generateStringKey();
		try {
			TEItemEnumerator enu = client.getItemEnumerator(parKey);
			while(enu.next())
			{
				TEItem item = enu.getItem();
				if(filter.accept(item))
				{
					String itemkey = (new ItemKey(item)).generateKey();
					itemKeyList.add(itemkey);
					System.out.println(itemkey);
				}
			}
			enu.close();
		} catch (TEItemEnumeratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			filter.close();
		} catch (TEItemFilterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
