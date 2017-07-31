package test.cic.textengine.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.client.TEClient;
import com.cic.textengine.client.TEItemEnumerator;
import com.cic.textengine.client.exception.TEClientException;
import com.cic.textengine.client.exception.TEItemEnumeratorException;
import com.cic.textengine.repository.type.ItemKey;

import junit.framework.TestCase;

public class TESpeedTest extends TestCase{

	private String itemKeyFile = "/home/CICDATA/opr_te/TESpeedTest/ItemKey.list";
	private String partitionKeyFile = "/home/CICDATA/opr_te/TESpeedTest/PartitionKey.list";
	private String testLog = "/home/CICDATA/opr_te/TESpeedTest/testLog.log";
	
	public void testPreparison()
	{
		// record the ItemKey in a file
		try {
			int count =0;
			int flushCount = 1000;
			FileWriter fw_itemkey = new FileWriter(itemKeyFile);
			PrintWriter pw = new PrintWriter(fw_itemkey);
			ArrayList<String> parKeyList = new ArrayList<String>();
			
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection con = DriverManager.getConnection(
					"jdbc:mysql://192.168.1.5/LeoProject91", "leo", "1q2w3e");
			Statement sm = con.createStatement();
			ResultSet rs;
			rs = sm
					.executeQuery("select ITEM_KEY FROM DS_ITEM WHERE ITEM_ID>=600000000");
			while(rs.next())
			{
				String tempStrKey = rs.getString("ITEM_KEY");
				ItemKey tempItemKey = ItemKey.decodeKey(tempStrKey);
				if(!parKeyList.contains(tempItemKey.getPartitionKey()))
					parKeyList.add(tempItemKey.getPartitionKey());
				pw.println(tempStrKey);
				count++;
				if(count%flushCount == 0)
					pw.flush();
			}
			con.close();
			pw.flush();
			pw.close();
			fw_itemkey.close();
			
			FileWriter fw_parkey = new FileWriter(partitionKeyFile);
			pw = new PrintWriter(fw_parkey);
			for(String key: parKeyList)
			{
				pw.println(key);
			}
			pw.close();
			fw_parkey.close();
			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void testGetItemSpeed()
	{
		try {
			TEClient client = new TEClient("192.168.2.2", 6869);
			FileReader fr = new FileReader(itemKeyFile);
			BufferedReader br = new BufferedReader(fr);
			String itemkey = null;
			long current = System.currentTimeMillis();
			long itemCount = 0;
			while((itemkey = br.readLine())!=null)
			{
				client.getItem(itemkey);
				itemCount ++;
			}
			current = System.currentTimeMillis() - current;
			br.close();
			fr.close();
			client.close();
			
			double ratio = (double)itemCount*1000/(double)current;
			FileWriter fw = new FileWriter(testLog, true);
			PrintWriter pw = new PrintWriter(fw);
			pw.println(String.format("It takes %s ms to get %s items from TE.", current, itemCount));
			pw.println(String.format("The average speed is %s item per second.", ratio));
			pw.println();
			
			pw.close();
			fw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TEClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void testEnumerateItemSpeed()
	{
		TEClient client = new TEClient("192.168.2.2", 6869);
		try {
			FileReader fr = new FileReader(itemKeyFile);
			BufferedReader br = new BufferedReader(fr);
			String parkey = null;
			long current = System.currentTimeMillis();
			long itemCount = 0;
			while((parkey=br.readLine())!=null)
			{
				TEItemEnumerator enu = client.getItemEnumerator(parkey);
				while(enu.next())
				{
					enu.getItem();
					itemCount++;
				}
				enu.close();
			}
			current = System.currentTimeMillis() - current;
			br.close();
			fr.close();
			
			double ratio = (double)itemCount*1000/(double)current;
			FileWriter fw = new FileWriter(testLog, true);
			PrintWriter pw = new PrintWriter(fw);
			pw.println(String.format("It takes %s to enumerate %s items from TE.", current, itemCount));
			pw.println(String.format("The average speed is %s item per second.", ratio));
			pw.println();
			
			pw.close();
			fw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TEItemEnumeratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String[] args)
	{
		TESpeedTest test = new TESpeedTest();
		test.testPreparison();
		test.testGetItemSpeed();
		test.testEnumerateItemSpeed();
	}
}
