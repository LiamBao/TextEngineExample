package test.cic.textengine.type;

import org.apache.commons.codec.DecoderException;
import org.junit.Test;

import com.cic.textengine.repository.type.PartitionKey;

public class PartitionKeyTest {
	@Test
	public void testdecode()
	{
		String parkey = "426c6f6732_2008__6";
		try {
			PartitionKey key = PartitionKey.decodeStringKey(parkey);
			System.out.println(String.format("SiteID:%s, ForumID:%s, Year:%s, Month:%s", key.getSiteID(), key.getForumID(), key.getYear(), key.getMonth()));
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}