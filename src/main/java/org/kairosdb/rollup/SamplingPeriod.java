package org.kairosdb.rollup;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Date;

public class SamplingPeriod
{
	private long startTime;
	private long endTime;

	public SamplingPeriod(long startTime, long endTime)
	{
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public long getStartTime()
	{
		return startTime;
	}

	public long getEndTime()
	{
		return endTime;
	}

	@Override
	public String toString()
	{
		return MoreObjects.toStringHelper(this)
				.add("startTime", new Date(startTime))
				.add("endTime", new Date(endTime))
				.toString();
	}
}
