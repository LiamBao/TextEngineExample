package com.cic.textengine.repository.namenode;

public class NameNodeConst {

	public static final byte CMD_PING = 0x01;
	public static final byte CMD_SHUTDOWN = 0x02;
	public static final byte CMD_APPLY_PARTITION_WRITE_LOCK = 0x11;
	public static final byte CMD_RELEASE_PARTITION_WRITE_LOCK = 0x12;
	public static final byte CMD_GET_DN_PARTITION_APPEND_POINT = 0x13;
	public static final byte CMD_GET_DN_CLIENT_FOR_WRITING = 0x14;
	public static final byte CMD_GET_DN_CLIENT_FOR_QUERY = 0x15;
	public static final byte CMD_GET_DN_PARTITION_ITEM_COUNT = 0x16;
	public static final byte CMD_CLEAN_PARTITION = 0x17;
	public static final byte CMD_GET_NEXT_DN_PARTITION_OPERATION = 0x18;
	public static final byte CMD_UPDATE_DN_PARTITION_VERSION = 0x19;
	public static final byte CMD_DEACTIVATE_DATA_NODE = 0x20;
	public static final byte CMD_GET_DN_ADDRESS_FOR_QUERY = 0x21;
	public static final byte CMD_GET_DN_ADDRESS_FOR_APPEND = 0x22;
	public static final byte CMD_CLEAN_NN_CACHE = 0x23;
	public static final byte CMD_GET_DN_LIST_FOR_QUERY = 0X24;

	public final static int ERROR_SUCCESS = 1; //success
	public final static int ERROR_GENERAL = 2; //general error
	public final static int ERROR_UNKNOWN = 3; //unknown error
	public static final int ERROR_PARTITION_LOCK_NOT_MATCH = 4;
	public static final int ERROR_PARTITION_LOCK_NOT_FOUND = 5;
}
