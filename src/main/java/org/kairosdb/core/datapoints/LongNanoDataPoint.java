package org.kairosdb.core.datapoints;

public class LongNanoDataPoint extends LongDataPoint
{
	private final long m_rawTime;

	private static long convertToMillis(long timestamp)
	{
		return timestamp / 1_000_000L;
	}

	public LongNanoDataPoint(long timestamp, long value)
	{
		super(convertToMillis(timestamp), value);
		m_rawTime = timestamp;
	}

	@Override
	public String getDataStoreDataType()
	{
		return LongNanoDataPointFactoryImpl.DST_LONG_NANO;
	}
}
