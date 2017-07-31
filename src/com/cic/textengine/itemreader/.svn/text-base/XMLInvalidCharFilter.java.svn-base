package com.cic.textengine.itemreader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class XMLInvalidCharFilter extends InputStreamReader {

	public XMLInvalidCharFilter(InputStream fInput, String encoding)
			throws FileNotFoundException, UnsupportedEncodingException {
		super(fInput, encoding);
	}

	@Override
	public void close() throws IOException {
		super.close();
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		int size = super.read(cbuf, off, len);
		for (int i = off; i < off + size; i++) {
			char c = cbuf[i];
			if ((c >= 0x00 && c <= 0x08) || (c >= 0x0e && c <= 0x1f)
					|| (c >= 0x0b && c <= 0x0c) || (c == 0xffff)) {
				cbuf[i] = 0x20;
			}
		}
		return size;
	}
}
