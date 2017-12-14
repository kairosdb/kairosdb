package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.ThreadReporter;

import java.util.*;
import java.util.concurrent.ExecutionException;

class CQLFilteredRowKeyIterator implements Iterator<DataPointsRowKey>
{
	private final SetMultimap<String, String> m_filterTags;
	private DataPointsRowKey m_nextKey;
	private final Iterator<ResultSet> m_resultSets;
	private ResultSet m_currentResultSet;
	private final String m_metricName;
	private final String m_clusterName;
	private int m_rawRowKeyCount = 0;


	public CQLFilteredRowKeyIterator(ClusterConnection cluster,
			String metricName,
			long startTime,
			long endTime,
			SetMultimap<String, String> filterTags) throws DatastoreException
	{
		m_filterTags = filterTags;
		m_metricName = metricName;
		m_clusterName = cluster.getClusterName();
		List<ResultSetFuture> futures = new ArrayList<>();
		long timerStart = System.currentTimeMillis();

		//Legacy key index - index is all in one row
		if ((startTime < 0) && (endTime >= 0))
		{
			BoundStatement negStatement = new BoundStatement(cluster.psRowKeyIndexQuery);
			negStatement.setBytesUnsafe(0, CassandraDatastore.serializeString(metricName));
			setStartEndKeys(negStatement, metricName, startTime, -1L);
			negStatement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			ResultSetFuture future = cluster.executeAsync(negStatement);

			futures.add(future);

			BoundStatement posStatement = new BoundStatement(cluster.psRowKeyIndexQuery);
			posStatement.setBytesUnsafe(0, CassandraDatastore.serializeString(metricName));
			setStartEndKeys(posStatement, metricName, 0L, endTime);
			posStatement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			future = cluster.executeAsync(posStatement);

			futures.add(future);

		}
		else
		{
			BoundStatement statement = new BoundStatement(cluster.psRowKeyIndexQuery);
			statement.setBytesUnsafe(0, CassandraDatastore.serializeString(metricName));
			setStartEndKeys(statement, metricName, startTime, endTime);
			statement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			ResultSetFuture future = cluster.executeAsync(statement);

			futures.add(future);
		}

		//System.out.println();
		//New index query index is broken up by time tier
		List<Long> queryKeyList = createQueryKeyList(cluster, metricName, startTime, endTime);
		for (Long keyTime : queryKeyList)
		{

			BoundStatement statement = new BoundStatement(cluster.psRowKeyQuery);
			statement.setString(0, metricName);
			statement.setTimestamp(1, new Date(keyTime));
			statement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			//printHosts(m_loadBalancingPolicy.newQueryPlan(m_keyspace, statement));

			futures.add(cluster.executeAsync(statement));
		}

		ListenableFuture<List<ResultSet>> listListenableFuture = Futures.allAsList(futures);

		try
		{
			m_resultSets = listListenableFuture.get().iterator();
			if (m_resultSets.hasNext())
				m_currentResultSet = m_resultSets.next();

			ThreadReporter.addDataPoint(CassandraDatastore.KEY_QUERY_TIME, System.currentTimeMillis() - timerStart);
		}
		catch (InterruptedException e)
		{
			throw new DatastoreException("Index query interrupted", e);
		}
		catch (ExecutionException e)
		{
			throw new DatastoreException("Failed to read key index", e);
		}
	}

	private DataPointsRowKey nextKeyFromIterator(ResultSet iterator)
	{
		DataPointsRowKey next = null;
		boolean newIndex = false;
		if (iterator.getColumnDefinitions().contains("row_time"))
			newIndex = true;

outer:
		while (!iterator.isExhausted())
		{
			DataPointsRowKey rowKey;
			Row record = iterator.one();
			m_rawRowKeyCount ++;

			if (newIndex)
				rowKey = new DataPointsRowKey(m_metricName, m_clusterName, record.getTimestamp(0).getTime(),
						record.getString(1), new TreeMap<String, String>(record.getMap(2, String.class, String.class)));
			else
				rowKey = CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER.fromByteBuffer(record.getBytes(0), m_clusterName);

			Map<String, String> keyTags = rowKey.getTags();
			for (String tag : m_filterTags.keySet())
			{
				String value = keyTags.get(tag);
				if (value == null || !m_filterTags.get(tag).contains(value))
					continue outer; //Don't want this key
			}

			next = rowKey;
			break;
		}

		return (next);
	}

	private List<Long> createQueryKeyList(ClusterConnection cluster, String metricName,
			long startTime, long endTime)
	{
		List<Long> ret = new ArrayList<>();

		if (cluster.psRowKeyTimeQuery != null) //cluster may be old
		{
			BoundStatement statement = new BoundStatement(cluster.psRowKeyTimeQuery);
			statement.setString(0, metricName);
			statement.setTimestamp(1, new Date(CassandraDatastore.calculateRowTime(startTime)));
			statement.setTimestamp(2, new Date(endTime));
			statement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			//printHosts(m_loadBalancingPolicy.newQueryPlan(m_keyspace, statement));

			ResultSet rows = cluster.execute(statement);

			while (!rows.isExhausted())
			{
				ret.add(rows.one().getTimestamp(0).getTime());
			}
		}

		return ret;
	}

	private void setStartEndKeys(
			BoundStatement boundStatement,
			String metricName, long startTime, long endTime)
	{
		DataPointsRowKey startKey = new DataPointsRowKey(metricName, m_clusterName,
				CassandraDatastore.calculateRowTime(startTime), "");

		DataPointsRowKey endKey = new DataPointsRowKey(metricName, m_clusterName,
				CassandraDatastore.calculateRowTime(endTime), "");
		endKey.setEndSearchKey(true);

		boundStatement.setBytesUnsafe(1, CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(startKey));
		boundStatement.setBytesUnsafe(2, CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(endKey));
	}

	@Override
	public boolean hasNext()
	{
		if (m_nextKey != null)
			return true;

		while (m_currentResultSet != null && (!m_currentResultSet.isExhausted() || m_resultSets.hasNext()))
		{
			m_nextKey = nextKeyFromIterator(m_currentResultSet);

			if (m_nextKey != null)
				break;

			if (m_resultSets.hasNext())
				m_currentResultSet = m_resultSets.next();
		}

		if (m_nextKey == null)
		{
			//todo make this a common atomic value
			ThreadReporter.addDataPoint(CassandraDatastore.RAW_ROW_KEY_COUNT, m_rawRowKeyCount);
		}

		return (m_nextKey != null);
	}

	@Override
	public DataPointsRowKey next()
	{
		DataPointsRowKey ret = m_nextKey;
		m_nextKey = null;
		return ret;
	}

	@Override
	public void remove()
	{
	}
}
