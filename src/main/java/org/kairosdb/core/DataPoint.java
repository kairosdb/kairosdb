package org.kairosdb.core;

import java.nio.ByteBuffer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/26/13
 Time: 8:46 AM
 To change this template use File | Settings | File Templates.
 */
public interface DataPoint extends Comparable<DataPoint>
{
	public long getTimestamp();
	public ByteBuffer toByteBuffer();
	public String toString();
	public int compareTo(DataPoint dp);
}
