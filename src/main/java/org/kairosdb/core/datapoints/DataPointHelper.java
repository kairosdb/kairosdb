package org.kairosdb.core.datapoints;

import org.kairosdb.core.DataPoint;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/31/13
 Time: 7:16 AM
 To change this template use File | Settings | File Templates.
 */
public abstract class DataPointHelper implements DataPoint
{
	protected long m_timestamp;

	public DataPointHelper(long timestamp)
	{
		m_timestamp = timestamp;
	}

	/**
	 Get the timestamp for this data point in milliseconds
	 @return timestamp
	 */
	public long getTimestamp()
	{
		return m_timestamp;
	}

}
