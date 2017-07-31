package com.cic.textengine.itemreader;

import com.cic.data.Item;
import com.cic.data.ItemMeta;
import com.cic.textengine.type.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.sql.*;

/*This class only use in ProcessParseXML.java temporarily */
public class XMLThreadItem extends TEItem {
	public XMLThreadItem() {
		super.setMeta(new TEItemMeta());
	}
	
	private HashMap<String,String> fields = new HashMap<String,String>();
	public void setValue(String fieldName, String fieldValue) {
		fields.put(fieldName, fieldValue);
	}
	
	public byte[] digest() {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		DataOutput output = new DataOutputStream(stream);
		try {
			output.writeUTF("FFT");
			output.writeUTF(getMeta().getItemUrl());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		byte[] binData = stream.toByteArray();
		synchronized (DIGESTER) {
	      DIGESTER.update(binData);
	      return DIGESTER.digest();
	    }
	}
	
	//*** Addtion methods
	public void convertTEItem() {
		for (int i=0; i < FIELDS_TEItem.length; i++) {
			String fieldName = FIELDS_TEItem[i];
			String tsValue;
			if (fieldName.equals("DateOfPost"))
				tsValue = fields.containsKey(fieldName) ? fields.get(fieldName) : 
					(fields.containsKey("FirstExtractionDate") ? fields.get("FirstExtractionDate") : "");
			else
				tsValue = fields.containsKey(fieldName) ? fields.get(fieldName) : "";
			super.setValue(fieldName, tsValue);
		}
	}
	private final static String[] FIELDS_TEItem = new String[] { "SiteID", "ForumID",
		"ThreadID", "Poster", "DateOfPost", "TopicPost", "ItemUrl",
		"SiteName", "ForumName", "ForumUrl", "FirstExtractionDate",
		"LatestExtractionDate", "PosterID", "PosterUrl", "ItemType",
		"Source", "KeywordGroup", "Keyword", "Subject", "Content" 
		//,"PageView","NumOfReplies","DateOfLastReply"
	};
	
	public String getXmlValue(String fieldName) {
		return fields.containsKey(fieldName) ? fields.get(fieldName) : null;
	}
	
	public String getItemKey() {
		return "FFT/" + fields.get("ItemUrl");
	}
}
