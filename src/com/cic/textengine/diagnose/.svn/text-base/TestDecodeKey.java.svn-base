package com.cic.textengine.diagnose;

import org.apache.commons.codec.DecoderException;

import com.cic.textengine.repository.type.ItemKey;
import com.cic.textengine.repository.type.PartitionKey;

public class TestDecodeKey {
	
	public static void testDecodePartitionKey(String key) throws DecoderException{
		PartitionKey parkey = null;
		parkey = PartitionKey.decodeStringKey(key);

		int year = parkey.getYear();
		int month = parkey.getMonth();
		String siteid = parkey.getSiteID();
		String forumid = parkey.getForumID();
		
		System.out.println(String.format("SITE_ID='%s' AND FORUM_ID='%s' AND THE_YEAR='%s' AND THE_MONTH='%s'", siteid, forumid, year, month));
	}
	
	public static void testDecodeItemKey(String key) throws DecoderException{
		ItemKey itemkey = null;
		itemkey = ItemKey.decodeKey(key);

		int year = itemkey.getYear();
		int month = itemkey.getMonth();
		String siteid = itemkey.getSiteID();
		String forumid = itemkey.getForumID();
		
		System.out.println(String.format("s: %s, f: %s, y: %s, m: %s", siteid, forumid, year, month));
	}

	/**
	 * @param args
	 * @throws DecoderException 
	 */
	public static void main(String[] args) throws DecoderException {
		if(args.length < 2){
			System.out.println("Usage: method key.");
			System.out.println("2 methods supported: parkey, itemkey");
			return;
		}
		if(args[0].trim().equalsIgnoreCase("parkey")){
			testDecodePartitionKey(args[1].trim());
		} else if (args[0].trim().equalsIgnoreCase("itemkey")){
			testDecodeItemKey(args[1].trim());
		}
	}

}
