package org.kairosdb.core;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.kairosdb.core.datapoints.NullDataPoint;

public abstract class DataPointMatcher extends BaseMatcher<DataPoint>
{

	public static Matcher<DataPoint> dataPoint(long timestamp, Object value)
	{
		if (value instanceof Long)
		{
			return new LongDataPointMatcher(timestamp, (Long)value);
		}
		else if (value == null)
		{
			return new NullDataPointMatcher(timestamp);
		}
		else
		{
			throw new IllegalArgumentException("Can't create a matcher for " + value);
		}
	}

	private static class NullDataPointMatcher extends DataPointMatcher {

		private final long timestamp;

		public NullDataPointMatcher(long timestamp)
		{
			this.timestamp = timestamp;
		}

		@Override
		public boolean matches(Object item)
		{
			if (item instanceof NullDataPoint) {
				return ((NullDataPoint) item).getTimestamp() == timestamp;
			}
			else
			{
				return false;
			}
		}

		@Override
		public void describeTo(Description description)
		{
			description.appendText("Null data point at timestamp ")
					.appendValue(timestamp);
		}
	}

	private static class LongDataPointMatcher extends DataPointMatcher
	{

		private final long timestamp;
		private final long value;

		public LongDataPointMatcher(long timestamp, long value)
		{
			this.timestamp = timestamp;
			this.value = value;
		}

		@Override
		public boolean matches(Object item)
		{
			if (item instanceof DataPoint)
			{
				DataPoint point = (DataPoint) item;
				return point.getTimestamp() == timestamp
						&& point.isLong()
						&& point.getLongValue() == value;
			}
			else
			{
				return false;
			}
		}

		@Override
		public void describeTo(Description description)
		{
			description.appendText("Long data point at timestamp ")
					.appendValue(timestamp)
					.appendText(" with value ")
					.appendValue(value);
		}
	}

}
