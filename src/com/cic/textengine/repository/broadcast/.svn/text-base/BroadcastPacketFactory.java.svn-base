package com.cic.textengine.repository.broadcast;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.cic.textengine.repository.broadcast.exception.IllegalBroadcastPackageException;

public class BroadcastPacketFactory {
	public static BroadcastPacket retrieveBraodcastPackage(InputStream is) 
	throws IllegalBroadcastPackageException, IOException{
		//try to read package header
		DataInputStream dis = new DataInputStream(is);
		int tag = dis.readInt();
		if (tag!=BroadcastPacket.TR_BROADCAST_PACKAGE_TAG){
			throw new IllegalBroadcastPackageException();
		}
		BroadcastPacket pkg = null;
		byte type = dis.readByte();
		switch(type){
			case BroadcastPacket.TYPE_BCPKG_DN_KEY:
				DNKeyPacket pkg_instance = new DNKeyPacket();
				pkg_instance.readBody(is);
				pkg = pkg_instance;
				break;
			default:
				throw new IllegalBroadcastPackageException();
		}
		return pkg;
	}
}
