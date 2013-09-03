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

	//@Override
	public int compareTo(DataPoint o)
	{
		long ret = getTimestamp() - o.getTimestamp();

		if (ret == 0L)
		{  //Simple hack to break a tie.
			ret = System.identityHashCode(this) - System.identityHashCode(o);
		}

		return (ret < 0L ? -1 : 1);
	}
}
