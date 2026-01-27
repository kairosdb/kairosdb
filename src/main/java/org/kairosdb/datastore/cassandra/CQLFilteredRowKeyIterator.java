package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.cql.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.metrics4j.MetricSourceManager;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import static org.kairosdb.core.KairosConfigProperties.QUERIES_REGEX_PREFIX;
import static org.kairosdb.datastore.cassandra.ClusterConnection.DATA_POINTS_TABLE_NAME;

public class CQLFilteredRowKeyIterator implements Iterator<DataPointsRowKey>
{
	private static final CassandraStats stats = MetricSourceManager.getSource(CassandraStats.class);

	private final SetMultimap<String, String> m_filterTags;
	private final Set<String> m_filterTagNames;
	private final String m_metricName;
	private final String m_clusterName;
	private final RowSpec m_rowSpec;
	private final Iterator<DataPointsRowKey> m_iterator;
	private int m_rawRowKeyCount = 0;
	private Map<String, Pattern> m_patternFilter;
	private Object m_rowKeySetLock = new Object();  //Used to synchronize loading of the below set when loaded async
	private Set<DataPointsRowKey> m_returnedKeys;  //Contains row keys this iterator will return from.


	@Inject
	public CQLFilteredRowKeyIterator(
			@Assisted ClusterConnection cluster,
			@Assisted String metricName,
			@Assisted("startTime") long startTime,
			@Assisted("endTime") long endTime,
			@Assisted SetMultimap<String, String> filterTags,
			@Named(QUERIES_REGEX_PREFIX) String regexPrefix) throws DatastoreException
	{
		m_filterTags = HashMultimap.create();
		m_filterTagNames = new HashSet<>();
		m_patternFilter = new HashMap<>();
		m_rowSpec = cluster.getRowSpec();

		AsyncResultCollector collector = new AsyncResultCollector();

		//Set of tags to pass to the RowKeyResultSetProcessor, it cannot contain
		//tags that are also specified as regex values
		HashMultimap<String, String> processorTags = HashMultimap.create();

		for (Map.Entry<String, String> entry : filterTags.entries())
		{
			String tag = entry.getKey();
			if (regexPrefix.length() != 0 && entry.getValue().startsWith(regexPrefix))
			{
				String regex = entry.getValue().substring(regexPrefix.length());

				Pattern pattern = Pattern.compile(regex);

				m_patternFilter.put(tag, pattern);
			}
			else
			{
				m_filterTags.put(tag, entry.getValue());

			}

			m_filterTagNames.add(tag);
		}


		m_metricName = metricName;
		m_clusterName = cluster.getClusterName();
		m_returnedKeys = new HashSet<>();
		long timerStart = System.currentTimeMillis();

		//Legacy key index - index is all in one row
		if ((startTime < 0) && (endTime >= 0))
		{
			BoundStatementBuilder negStatement = cluster.psRowKeyIndexQuery.boundStatementBuilder();
			negStatement.setBytesUnsafe(0, CassandraDatastore.serializeString(metricName));
			setStartEndKeys(negStatement, metricName, startTime, -1L);
			negStatement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			CompletionStage<AsyncResultSet> future = cluster.executeAsync(negStatement.build());

			collector.addResultSet(future, this::loadKeyFromOldIndex);

			BoundStatementBuilder posStatement = cluster.psRowKeyIndexQuery.boundStatementBuilder()
					.setBytesUnsafe(0, CassandraDatastore.serializeString(metricName));

			setStartEndKeys(posStatement, metricName, 0L, endTime);
			posStatement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			future = cluster.executeAsync(posStatement.build());
			collector.addResultSet(future, this::loadKeyFromOldIndex);
		}
		else
		{
			BoundStatementBuilder statement = cluster.psRowKeyIndexQuery.boundStatementBuilder()
					.setBytesUnsafe(0, CassandraDatastore.serializeString(metricName));
			setStartEndKeys(statement, metricName, startTime, endTime);
			statement.setConsistencyLevel(cluster.getReadConsistencyLevel());

			CompletionStage<AsyncResultSet> future = cluster.executeAsync(statement.build());
			collector.addResultSet(future, this::loadKeyFromOldIndex);
		}

		//System.out.println();
		//New index query index is broken up by time tier
		RowKeyLookup rowKeyLookup = cluster.getRowKeyLookupForMetric(metricName);
		List<Long> queryKeyList = createQueryKeyList(cluster, metricName, startTime, endTime);
		for (Long keyTime : queryKeyList)
		{
			CompletionStage<AsyncResultSet> future = rowKeyLookup.queryRowKeys(metricName, keyTime, m_filterTags);
			collector.addResultSet(future, this::loadKeyFromNewIndex);
		}

		collector.join();

		if (collector.hasThrownException())
		{
			Throwable thrownException = collector.getThrownException();
			throw new DatastoreException("Error querying keys", thrownException);
		}

		m_iterator = m_returnedKeys.iterator();

		stats.keyQueryTime().put(Duration.ofMillis(System.currentTimeMillis() - timerStart));
	}

	private void addRowKey(DataPointsRowKey rowKey)
	{
		Map<String, String> keyTags = rowKey.getTags();
		for (String tag : m_filterTagNames)
		{
			String value = keyTags.get(tag);
			if (value == null || !(m_filterTags.get(tag).contains(value) ||
					matchRegexFilter(tag, value)))
				return; //Don't want this key
		}

		m_returnedKeys.add(rowKey);
	}


	private void loadKeyFromNewIndex(Row record)
	{
		if (record.getString(1) == null)
			return;

		DataPointsRowKey rowKey = new DataPointsRowKey(m_metricName, m_clusterName, record.getInstant(0).toEpochMilli(),
				record.getString(1), new TreeMap<String, String>(record.getMap(2, String.class, String.class)));

		rowKey.setTtl(record.getInt(3));
		addRowKey(rowKey);
	}

	private void loadKeyFromOldIndex(Row record)
	{
		DataPointsRowKey rowKey = CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER.fromByteBuffer(record.getByteBuffer(0), m_clusterName);
		addRowKey(rowKey);
	}

	private boolean matchRegexFilter(String tag, String value)
	{
		if (m_patternFilter.containsKey(tag))
		{
			Pattern pattern = m_patternFilter.get(tag);

			return pattern.matcher(value).matches();
		}
		return false;
	}


	private List<Long> createQueryKeyList(ClusterConnection cluster, String metricName,
			long startTime, long endTime)
	{
		List<Long> ret = new ArrayList<>();

		if (cluster.psRowKeyTimeQuery != null) //cluster may be old
		{
			BoundStatement statement = cluster.psRowKeyTimeQuery.boundStatementBuilder()
					.setString(0, metricName)
					.setString(1, DATA_POINTS_TABLE_NAME)
					.setInstant(2, Instant.ofEpochMilli(m_rowSpec.calculateRowTime(startTime)))
					.setInstant(3, Instant.ofEpochMilli(endTime))
					.setConsistencyLevel(cluster.getReadConsistencyLevel())
					.build();

			//printHosts(m_loadBalancingPolicy.newQueryPlan(m_keyspace, statement));

			ResultSet rows = cluster.execute(statement);

			for (Row record : rows)
				ret.add(record.getInstant(0).toEpochMilli());
		}

		return ret;
	}

	private void setStartEndKeys(
			BoundStatementBuilder boundStatement,
			String metricName, long startTime, long endTime)
	{
		DataPointsRowKey startKey = new DataPointsRowKey(metricName, m_clusterName,
				m_rowSpec.calculateRowTime(startTime), "");

		DataPointsRowKey endKey = new DataPointsRowKey(metricName, m_clusterName,
				m_rowSpec.calculateRowTime(endTime), "");
		endKey.setEndSearchKey(true);

		boundStatement.setBytesUnsafe(1, CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(startKey));
		boundStatement.setBytesUnsafe(2, CassandraDatastore.DATA_POINTS_ROW_KEY_SERIALIZER.toByteBuffer(endKey));
	}

	@Override
	public boolean hasNext()
	{
		return m_iterator.hasNext();
	}

	@Override
	public DataPointsRowKey next()
	{
		return m_iterator.next();
	}

	@Override
	public void remove()
	{
	}
}
