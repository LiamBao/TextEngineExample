package com.cic.textengine.type;

import java.io.*;

import com.cic.data.impl.BaseItemImpl;
import com.cic.textengine.repository.type.ItemKey;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class TEItem extends BaseItemImpl implements Comparable<TEItem>, Externalizable {
	private final static byte CUR_VERSION = 1;

	public void readFields(DataInput in) throws IOException {
		in.readByte();
		TEItemMeta meta = new TEItemMeta();
		meta.readFields(in);
		String subject = in.readUTF();
		int len = 0;
		len = in.readInt();
		char ch[] = new char[len];
		
		for (int i = 0;i< len ;i++){
			ch[i] = in.readChar();
		}
		String content = new String(ch);

		this.setMeta(meta);
		this.setSubject(subject);
		this.setContent(content);
	}

	public void write(DataOutput out) throws IOException {
	  out.writeByte(CUR_VERSION);                   // store current version
	  ((TEItemMeta)getMeta()).write(out);
	  if(this.getSubject() != null)
	  {
		  out.writeUTF(this.getSubject());
	  }
	  else
		  out.writeUTF("");
	  if(this.getContent()!=null)
	  {
		  out.writeInt(this.getContent().length());
		  out.writeChars(this.getContent());
	  }
	  else
		  out.writeInt(0);
	}

	public void writeExternal(ObjectOutput out) throws IOException
	{
		write(out);
	}

	public void readExternal(ObjectInput in) throws IOException , ClassNotFoundException
	{
		readFields(in);
	}
	
	public int compareTo(TEItem that) {
		long diff = this.getMeta().getItemID() - that.getMeta().getItemID();
		if (diff > 0)
			return 1;
		if (diff == 0)
			return 0;
		/* if (diff < 0) */
		return -1;	
	}
	
	  protected static final MessageDigest DIGESTER;

	  static {

	    try {

	      DIGESTER = MessageDigest.getInstance("MD5");

	    } catch (NoSuchAlgorithmException e) {

	      throw new RuntimeException(e);

	    }

	  }

	 
	public byte[] digest() {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		DataOutput output = new DataOutputStream(stream);
		try {
			output.writeLong(getMeta().getDateOfPost());
			String forumID = getMeta().getForumID();
			output.writeUTF(forumID == null ? "" : forumID);
			output.writeBoolean(getMeta().isTopicPost());
			String itemUrl = getMeta().getItemUrl();
			output.writeUTF(itemUrl == null ? "" : itemUrl);
			String keyword = getMeta().getKeyword();
			output.writeUTF(keyword == null ? "" : keyword);
			String keywordGroup = getMeta().getKeywordGroup();
			output.writeUTF(keywordGroup == null ? "" : keywordGroup);
			String poster = getMeta().getPoster();
			output.writeUTF(poster == null ? "" : poster);
			output.writeLong(getMeta().getSiteID());
			String source = getMeta().getSource();
			output.writeUTF(source == null ? "" : source);
			String subject = getSubject();
			output.writeUTF(subject == null ? "" : subject);
			output.writeLong(getMeta().getThreadID());
			String content = getContent();
			// the dataoutputstream's writeUTF method will not output more than 65535 bytes.
			// so if the content is longer than 65535 bytes we will ignore the content after that length.
			int strlen = 0;
			if(content != null)
			{
				strlen = content.length();
				int c = 0;
				int utflen = 0;
				for (int i = 0; i < strlen; i++) {
					c = content.charAt(i);
				   if ((c >= 0x0001) && (c <= 0x007F)) {
					   utflen++;
				   } else if (c > 0x07FF) {
					   utflen += 3;
				   } else {
					   utflen += 2;
				   }
				   if(utflen >= 65535)
				   {
					   strlen = i;
					   content = content.substring(0, strlen);
					   break;
				   }
				}
			}
			output.writeUTF(content == null ? "" : content);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		byte[] binData = stream.toByteArray();
		synchronized (DIGESTER) {
	      DIGESTER.update(binData);
	      return DIGESTER.digest();
	    }
	}
	
	public String getItemKey()
	{
		
		return (new ItemKey(this.getMeta().getSource(), String.valueOf(this.getMeta()
				.getSiteID()), this.getMeta().getForumID(), this.getMeta()
				.getYearOfPost(), this.getMeta().getMonthOfPost(), this
				.getMeta().getItemID())).generateKey();
		
	}
}
