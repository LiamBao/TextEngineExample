package com.cic.textengine.diagnose;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.repository.datanode.client.DataNodeClient;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientCommunicationException;
import com.cic.textengine.repository.datanode.client.exception.DataNodeClientException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;

public class DeleteOnePost {

	/**
	 * @param args
	 * @throws DecoderException
	 * @throws NameNodeClientException
	 * @throws DataNodeClientCommunicationException
	 * @throws DataNodeClientException
	 */
	public static void main(String[] args) throws DecoderException,
			NameNodeClientException, DataNodeClientException,
			DataNodeClientCommunicationException {
		if (args.length < 1) {
			System.out.println("Usage: ItemKeys needed.");
			return;
		}

		String[] itemkeys = args[0].trim().split(",");
		HashMap<String, ArrayList<Long>> itemMap = new HashMap<String, ArrayList<Long>>();
		NameNodeClient nclient = new NameNodeClient("192.168.2.2", 6869);

		for (String itemkey : itemkeys) {
			ItemKey key = ItemKey.decodeKey(itemkey.trim());
			String parkey = key.getPartitionKey();
			ArrayList<Long> itemList = itemMap.get(parkey);
			if (itemList == null) {
				itemList = new ArrayList<Long>();
				itemMap.put(parkey, itemList);
			}
			itemList.add(key.getItemID());
		}

		for (String parkey : itemMap.keySet()) {
			PartitionKey par = PartitionKey.decodeStringKey(parkey);
			DataNodeClient dclient = nclient.getDNClientForWriting(
					par.getYear(), par.getMonth(), par.getSiteID(),
					par.getForumID());
			dclient.deleteItems(par.getYear(), par.getMonth(), par.getSiteID(),
					par.getForumID(), itemMap.get(parkey), true);
		}
		System.out.println("Delete success.");

	}

}
