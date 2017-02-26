package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;
import org.kairosdb.util.SimpleStats;

import java.io.IOException;
import java.util.*;

/**
 Created by bhawkins on 1/28/17.
 */
public class MemorySearchResult implements SearchResult
{
	private final String m_metricName;
	private final List<DataPointRow> m_dataPointRows;
	private MemoryDataPointRow m_currentRow;

	public MemorySearchResult(String metricName)
	{
		m_metricName = metricName;
		m_dataPointRows = new ArrayList<>();
	}

	@Override
	public List<DataPointRow> getRows()
	{
		return m_dataPointRows;
	}

	@Override
	public void addDataPoint(DataPoint datapoint) throws IOException
	{
		m_currentRow.addDataPoint(datapoint);
	}

	@Override
	public void startDataPointSet(String dataType, Map<String, String> tags) throws IOException
	{
		m_currentRow = new MemoryDataPointRow(dataType, tags);
		m_dataPointRows.add(m_currentRow);
	}

	@Override
	public void endDataPoints() throws IOException
	{
		if (m_currentRow != null)
			m_currentRow.endRow();
	}

	private class MemoryDataPointRow implements DataPointRow
	{
		private final String m_dataType;
		private final Map<String, String> m_tags;
		private final List<DataPoint> m_dataPoints;
		private Iterator<DataPoint> m_dataPointIterator;

		private MemoryDataPointRow(String dataType, Map<String, String> tags)
		{
			m_dataType = dataType;
			m_tags = tags;
			m_dataPoints = new ArrayList<>();
		}

		@Override
		public String getName()
		{
			return m_metricName;
		}

		@Override
		public String getDatastoreType()
		{
			return m_dataType;
		}

		@Override
		public Set<String> getTagNames()
		{
			return m_tags.keySet();
		}

		@Override
		public String getTagValue(String tag)
		{
			return m_tags.get(tag);
		}

		@Override
		public void close()
		{
		}

		public void endRow()
		{
			m_dataPointIterator = m_dataPoints.iterator();
		}

		@Override
		public int getDataPointCount()
		{
			return m_dataPoints.size();
		}

		public void addDataPoint(DataPoint dp)
		{
			m_dataPoints.add(dp);
		}

		@Override
		public boolean hasNext()
		{
			return m_dataPointIterator.hasNext();
		}

		@Override
		public DataPoint next()
		{
			return m_dataPointIterator.next();
		}

		@Override
		public void remove()
		{

		}
	}
}
