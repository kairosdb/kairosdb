package org.kairosdb.datastore.cassandra;

import org.kairosdb.core.datastore.TimeUnit;

public class RowSpec
	{
	public static final long DEFAULT_ROW_WIDTH = 1814400000L; //3 Weeks wide in milliseconds
	public static final String LEGACY_UNIT = "LEGACY";

	private final long m_rowWidth;
	private final TimeUnit m_rowUnit;
	private final boolean m_isLegacy;

	public RowSpec()
		{
		m_rowUnit = TimeUnit.MILLISECONDS;
		m_rowWidth = DEFAULT_ROW_WIDTH;
		m_isLegacy = true;
		}

	public RowSpec(long rowWidth, TimeUnit rowUnit, boolean isLegacy)
		{
		m_rowWidth = rowWidth;
		m_rowUnit = rowUnit;
		m_isLegacy = isLegacy;
		}

	public long getRowWidthInMillis()
		{
		if (m_rowUnit == TimeUnit.SECONDS)
			return m_rowWidth * 1000;
		else
			return m_rowWidth;
		}

	@SuppressWarnings("PointlessBitwiseExpression")
	public int getColumnName(long rowTime, long timestamp)
		{
		long columnTime = timestamp - rowTime;

		int ret;

		if (m_rowUnit == TimeUnit.SECONDS)
			{
			ret = (int) (columnTime / 1000);
			}
		else
			{
			ret = (int) (columnTime);
			/*
				The timestamp is shifted to support legacy datapoints that
				used the extra bit to determine if the value was long or double
			 */
			if (m_isLegacy)
				ret = ret << 1;
			}

		return (ret);
		}

	public long getColumnTimestamp(long rowTime, int columnName)
		{
		long columnTime = 0L;

		if (m_rowUnit == TimeUnit.SECONDS)
			{
			columnTime = columnName;
			columnTime *= 1000;
			}
		else
			{
			if (m_isLegacy)
				columnName = columnName >>> 1;

			columnTime = columnName;
			}

		return (rowTime + columnTime);
		}

	public long calculateRowTime(long timestamp)
		{
		return (timestamp - (Math.abs(timestamp) % m_rowWidth));
		}
	}
