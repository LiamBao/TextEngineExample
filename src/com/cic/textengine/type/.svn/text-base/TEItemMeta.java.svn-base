package com.cic.textengine.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TEItemMeta extends com.cic.data.impl.BaseItemMetaImpl {
	private final static byte CUR_VERSION = 1;

	public void setDateOfPost(long dateOfPost) {
		super.setDateOfPost(dateOfPost);
	}

	public void readFields(DataInput in) throws IOException {
		in.readByte(); // store current version
		setItemID(in.readLong());
		setItemUrl(in.readUTF());

		setThreadID(in.readLong());

		setForumID(in.readUTF());
		setForumName(in.readUTF());
		setForumUrl(in.readUTF());

		setSiteID(in.readLong());
		setSiteName(in.readUTF());

		setDateOfPost(in.readLong());

		setPoster(in.readUTF());
		setPosterID(in.readUTF());
		setPosterUrl(in.readUTF());

		setTopicPost(in.readBoolean());

		setSubject(in.readUTF());
		setKeyword(in.readUTF());
		setKeywordGroup(in.readUTF());

		setFirstExtractionDate(in.readLong());
		setLatestExtractionDate(in.readLong());

		setItemType(in.readUTF());
		setSource(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeByte(CUR_VERSION); // store current version
		out.writeLong(this.getItemID());
		// out.writeUTF(new
		// sun.misc.BASE64Encoder().encode(this.getItemUrl().getBytes()).toString());
		out.writeUTF(this.getItemUrl());

		out.writeLong(this.getThreadID());

		out.writeUTF(this.getForumID());
		out.writeUTF(this.getForumName());
		out.writeUTF(this.getForumUrl());

		out.writeLong(this.getSiteID());
		out.writeUTF(this.getSiteName());

		out.writeLong(this.getDateOfPost());

		out.writeUTF(this.getPoster());
		out.writeUTF(this.getPosterID());
		out.writeUTF(this.getPosterUrl());

		out.writeBoolean(this.isTopicPost());
		out.writeUTF(this.getSubject());
		out.writeUTF(this.getKeyword());
		out.writeUTF(this.getKeywordGroup());

		out.writeLong(this.getFirstExtractionDate());
		out.writeLong(this.getLatestExtractionDate());

		out.writeUTF(this.getItemType());
		out.writeUTF(this.getSource());
	}
}
