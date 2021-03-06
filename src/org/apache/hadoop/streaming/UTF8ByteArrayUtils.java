/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.streaming;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;

/**
 * General utils for byte array containing UTF-8 encoded strings
 * @author hairong 
 */

public class UTF8ByteArrayUtils {
  /**
   * Find the first occured tab in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param length no. of bytes
   * @return position that first tab occures otherwise -1
   */
  public static int findTab(byte [] utf, int start, int length) {
    for(int i=start; i<(start+length); i++) {
      if (utf[i]==(byte)'\t') {
        return i;
      }
    }
    return -1;      
  }
  
  /**
   * Find the first occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param end ending position
   * @param b the byte to find
   * @return position that first byte occures otherwise -1
   */
  public static int findByte(byte [] utf, int start, int end, byte b) {
    for(int i=start; i<end; i++) {
      if (utf[i]==b) {
        return i;
      }
    }
    return -1;      
  }
  
  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param end ending position
   * @param b the byte to find
   * @param n the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   */
  private static int findNthByte(byte [] utf, int start, int length, byte b, int n) {
    int pos = -1;
    int nextStart = start + 1;
    for (int i = 0; i < n; i++) {
      pos = findByte(utf, nextStart, length, b);
      if (pos < 0) {
        return pos;
      }
      nextStart = pos + 1;
    }
    return pos;      
  }
  
  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param b the byte to find
   * @param n the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   */
  public static int findNthByte(byte [] utf, byte b, int n) {
    return findNthByte(utf, 0, utf.length, b, n);      
  }
    
  /**
   * Find the first occured tab in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @return position that first tab occures otherwise -1
   */
  public static int findTab(byte [] utf) {
    return findNthByte(utf, 0, utf.length, (byte)'\t', 1);
  }

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param start starting offset
   * @param length no. of bytes
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @throws IOException
   */
  public static void splitKeyVal(byte[] utf, int start, int length, 
                                 Text key, Text val, int splitPos) throws IOException {
    if (splitPos<start || splitPos >= (start+length))
      throw new IllegalArgumentException("splitPos must be in the range " +
                                         "[" + start + ", " + (start+length) + "]: " + splitPos);
    int keyLen = (splitPos-start);
    byte [] keyBytes = new byte[keyLen];
    System.arraycopy(utf, start, keyBytes, 0, keyLen);
    int valLen = (start+length)-splitPos-1;
    byte [] valBytes = new byte[valLen];
    System.arraycopy(utf, splitPos+1, valBytes, 0, valLen);
    key.set(keyBytes);
    val.set(valBytes);
  }
    

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @throws IOException
   */
  public static void splitKeyVal(byte[] utf, Text key, Text val, int splitPos) 
    throws IOException {
    splitKeyVal(utf, 0, utf.length, key, val, splitPos);
  }
    
  /**
   * Read a utf8 encoded line from a data input stream. 
   * @param in data input stream
   * @return a byte array containing the line 
   * @throws IOException
   */
  public static byte [] readLine(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    long bytes = LineRecordReader.readLine(in, baos);
    baos.close();
    if (bytes <= 0)
      return null;
    return baos.toByteArray();
  }
}
