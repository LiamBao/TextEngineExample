package com.cic.textengine.diagnose;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.cic.textengine.posttrend.PostTrend;
import com.cic.textengine.repository.ItemImporter;
import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.RemoteTEItemEnumerator;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.PartitionKey;
import com.cic.textengine.type.TEItem;
import com.cic.textengine.utils.BloomFilterHelper;

public class CleanPartition {

	private PartitionKey parKey = null;
	private String nnAddr = null;
	private int nnPort = 0;

	private static Logger logger = Logger.getLogger(CleanPartition.class);

	public CleanPartition(String address, int port, int year, int month,
			String siteid, String forumid) {
		parKey = new PartitionKey(year, month, siteid, forumid);
		nnAddr = address;
		nnPort = port;
	}

	public void clean() {
		int year = parKey.getYear();
		int month = parKey.getMonth();
		String siteid = parKey.getSiteID();
		String forumid = parKey.getForumID();
		try {
			NameNodeClient nn_client = new NameNodeClient(nnAddr, nnPort);
			DataNodeClient dn_client = nn_client.getDNClientForWriting(year,
					month, siteid, forumid);

			// delete item from bloom filter
			RemoteTEItemEnumerator enu = dn_client.getItemEnumerator(year,
					month, siteid, forumid);
			while (enu.next()) {
				TEItem item = enu.getItem();
				BloomFilterHelper.remove(parKey, item);
			}

			logger.info(String.format("Delete all items of partition %s from Bloomfilter.", parKey.generateStringKey()));
			
			// clean partition from TR
			nn_client.cleanPartition(year, month, siteid, forumid);
			logger.info(String.format("Clean the partition %s", parKey.generateStringKey()));
			
			// update the post trend
			PostTrend post = new PostTrend();
			post.setTrend(parKey, 0);

		} catch (NameNodeClientException e) {
			logger.error("Error in clean the partition:"
					+ e.getLocalizedMessage());
		} catch (IOException e) {
			logger.error("Error in clean the partition:"
					+ e.getLocalizedMessage());
		} catch (DataNodeClientException e) {
			logger.error("Error in clean the partition:"
					+ e.getLocalizedMessage());
		} catch (DataNodeClientCommunicationException e) {
			logger.error("Error in clean the partition:"
					+ e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in clean the partition:"
					+ e.getLocalizedMessage());
		}

	}
	
	public static void main(String[] args)
	{
		if(args.length < 6)
		{
			System.out.println("6 parameters needed: NameNodeAddr NameNodePort year month siteid forumid");
			System.exit(0);
		}
		try {
			Configurer.config(ItemImporter.ITEM_IMPORTER_PROPERTIES);
		} catch (IOException e1) {
			logger.error("Error in load the configuration.");
			return;
		}
		String nnAddr = args[0];
		int nnPort = Integer.parseInt(args[1]);
		int year = Integer.parseInt(args[2]);
		int month = Integer.parseInt(args[3]);
		String siteid = args[4];
		String forumid = args[5];
		
		CleanPartition cp = new CleanPartition(nnAddr, nnPort, year, month, siteid, forumid);
		cp.clean();
		
		try {
			BloomFilterHelper.close();
		} catch (Exception e) {
			logger.error("Error in close bloom filter.");
		}
	}

}
