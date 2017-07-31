package com.cic.textengine.type;

public class SegmentInfo {
	/**
	 * The segment ID
	 */
	private int segmentID = 0;

	/**
	 * The itemid at the top of the segement
	 */
	private long start;

	/**
	 * The itemid at the bottom of the segment
	 */
	private long end;
	
	private int tag;

	public long getEnd() {
		return end;
	}

	public int getSegmentID() {
		return segmentID;
	}

	public long getStart() {
		return start;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setSegmentID(int segmentID) {
		this.segmentID = segmentID;
	}

	public void setStart(long start) {
		this.start = start;
	}
	
	public boolean equals(Object that){
		if (that == null)
			return false;
		
		if (this == that)
			return true;
		
		SegmentInfo thatInfo = (SegmentInfo)that;
		if (this.start == thatInfo.start && this.end == thatInfo.end && this.segmentID == thatInfo.segmentID)
			return true;
		return false;
	}
	
	public boolean equalsRange(Object that){
		if (that == null)
			return false;
		
		if (this == that)
			return true;
		
		SegmentInfo thatInfo = (SegmentInfo)that;
		if (this.start == thatInfo.start && this.end == thatInfo.end)
			return true;
		return false;
	}

	public int getTag() {
		return tag;
	}

	public void setTag(int tag) {
		this.tag = tag;
	}
}
