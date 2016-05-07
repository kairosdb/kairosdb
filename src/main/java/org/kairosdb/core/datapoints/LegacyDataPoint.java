package org.kairosdb.core.datapoints;

import org.kairosdb.core.datapoints.DataPointHelper;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/9/13
 Time: 12:32 PM
 To change this template use File | Settings | File Templates.
 */
public abstract class LegacyDataPoint extends DataPointHelper
{
	public LegacyDataPoint(long timestamp)
	{
		super(timestamp);
	}


	@Override
	public String getDataStoreDataType()
	{
		return LegacyDataPointFactory.DATASTORE_TYPE;
	}

	@Override
	public boolean isLong()
	{
		return false;
	}

	@Override
	public long getLongValue()
	{
		return 0;
	}

	@Override
	public boolean isDouble()
	{
		return false;
	}

	@Override
	public double getDoubleValue()
	{
		return 0;
	}
}
