package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;
import org.kairosdb.util.MemoryMonitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 Created by bhawkins on 1/28/17.
 */
public class MemorySearchResult implements SearchResult
{
	private final String m_metricName;
	private final List<DataPointRow> m_dataPointRows;
	private final MemoryMonitor m_memoryMonitor;


	public MemorySearchResult(String metricName)
	{
		m_metricName = metricName;
		m_dataPointRows = Collections.synchronizedList(new ArrayList<>());
		m_memoryMonitor = new MemoryMonitor(1000);
	}

	@Override
	public List<DataPointRow> getRows()
	{
		return m_dataPointRows;
	}


	@Override
	public DataPointWriter startDataPointSet(String dataType, SortedMap<String, String> tags) throws IOException
	{
		MemoryDataPointRow currentRow = new MemoryDataPointRow(dataType, tags);
		m_dataPointRows.add(currentRow);
		return new MemoryDataPointWriter(currentRow);
	}


	private class MemoryDataPointWriter implements DataPointWriter
	{
		private MemoryDataPointRow m_currentRow;

		public MemoryDataPointWriter(MemoryDataPointRow currentRow)
		{
			m_currentRow = currentRow;
		}

		@Override
		public void addDataPoint(DataPoint datapoint) throws IOException
		{
			m_currentRow.addDataPoint(datapoint);
			m_memoryMonitor.checkMemoryAndThrowException();
		}

		@Override
		public void close() throws IOException
		{
			m_currentRow.endRow();
		}
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
