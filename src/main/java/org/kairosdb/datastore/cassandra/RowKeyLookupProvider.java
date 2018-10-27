package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.Statement;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class RowKeyLookupProvider
{

	public static final Logger logger = LoggerFactory.getLogger(RowKeyLookupProvider.class);

	private final ClusterConnection clusterConnection;
	private final Set<String> tagIndexedLookupMetricNames;
	private final boolean alwaysUseTagIndexedLookup;

	@Inject
	public RowKeyLookupProvider(@Named("write_cluster") ClusterConnection clusterConnection,
								@Named(CassandraConfiguration.TAG_INDEXED_ROW_KEY_LOOKUP_METRICS) String tagIndexedRowKeyLookupMetrics)
	{
		this.clusterConnection = clusterConnection;
		this.tagIndexedLookupMetricNames = ImmutableSet.copyOf(
				Splitter.on(",").trimResults().split(Strings.nullToEmpty(tagIndexedRowKeyLookupMetrics)));
		this.alwaysUseTagIndexedLookup = tagIndexedLookupMetricNames.equals(ImmutableSet.of("*"));
		if (alwaysUseTagIndexedLookup)
		{
			logger.info("Using tag-indexed row key lookup for all metrics");
		}
		else if (tagIndexedLookupMetricNames.isEmpty())
		{
			logger.info("Indexed tag-indexed row key lookup is disabled");
		}
		else
		{
			logger.info("Using tag-indexed row key lookup for {}", tagIndexedLookupMetricNames);
		}
	}

	public RowKeyLookup getRowKeyLookupForMetric(String metricName)
	{
		if (alwaysUseTagIndexedLookup || tagIndexedLookupMetricNames.contains(metricName))
		{
			logger.debug("Using tag-indexed row key lookup for {}", metricName);
			return new TagIndexedRowKeysTableLookup(clusterConnection);
		}
		else
		{
			logger.debug("Using standard row key lookup for {}", metricName);
			return new RowKeysTableLookup(clusterConnection);
		}
	}


	static class RowKeysTableLookup implements RowKeyLookup
	{

		private final ClusterConnection clusterConnection;

		public RowKeysTableLookup(ClusterConnection clusterConnection)
		{
			this.clusterConnection = clusterConnection;
		}

		@Override
		public List<Statement> createInsertStatements(DataPointsRowKey rowKey, int rowKeyTtl)
		{
			return ImmutableList.of(
					clusterConnection.psRowKeyInsert.bind()
							.setString(0, rowKey.getMetricName())
							.setTimestamp(1, new Date(rowKey.getTimestamp()))
							.setString(2, rowKey.getDataType())
							.setMap(3, rowKey.getTags())
							.setInt(4, rowKeyTtl)
							.setIdempotent(true));
		}

		@Override
		public List<Statement> createDeleteStatements(DataPointsRowKey rowKey)
		{
			return ImmutableList.of(
					clusterConnection.psRowKeyDelete.bind()
							.setString(0, rowKey.getMetricName())
							.setTimestamp(1, new Date(rowKey.getTimestamp()))
							.setString(2, rowKey.getDataType())
							.setMap(3, rowKey.getTags()));
		}

		@Override
		public List<Statement> createQueryStatements(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			return ImmutableList.of(
					clusterConnection.psRowKeyQuery.bind()
							.setString(0, metricName)
							.setTimestamp(1, new Date(rowKeyTimestamp)));
		}
	}

	static class TagIndexedRowKeysTableLookup implements RowKeyLookup
	{

		private static final int WILDCARD_TAG_HASH_VALUE = 0;

		private final ClusterConnection clusterConnection;

		public TagIndexedRowKeysTableLookup(ClusterConnection clusterConnection)
		{
			this.clusterConnection = clusterConnection;
		}

		@Override
		public List<Statement> createInsertStatements(DataPointsRowKey rowKey, int rowKeyTtl)
		{
			Set<Integer> tagPairHashes = generateTagPairHashes(rowKey);
			List<Statement> insertStatements = new ArrayList<>(tagPairHashes.size());
			Date rowKeyTimestamp = new Date(rowKey.getTimestamp());
			for (Integer tagPairHash : tagPairHashes)
			{
				insertStatements.add(
						clusterConnection.psTagIndexedRowKeyInsert.bind()
								.setString(0, rowKey.getMetricName())
								.setTimestamp(1, rowKeyTimestamp)
								.setString(2, rowKey.getDataType())
								.setInt(3, tagPairHash)
								.setMap(4, rowKey.getTags())
								.setInt(5, rowKeyTtl)
								.setIdempotent(true));
			}
			return insertStatements;
		}

		@Override
		public List<Statement> createDeleteStatements(DataPointsRowKey rowKey)
		{
			Set<Integer> tagPairHashes = generateTagPairHashes(rowKey);
			List<Statement> deleteStatements = new ArrayList<>(tagPairHashes.size());
			Date rowKeyTimestamp = new Date(rowKey.getTimestamp());
			for (Integer tagPairHash : tagPairHashes) {
				deleteStatements.add(
						clusterConnection.psTagIndexedRowKeyDelete.bind()
								.setString(0, rowKey.getMetricName())
								.setTimestamp(1, rowKeyTimestamp)
								.setString(2, rowKey.getDataType())
								.setInt(3, tagPairHash)
								.setMap(4, rowKey.getTags()));
			}
			return deleteStatements;
		}

		@Override
		public List<Statement> createQueryStatements(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			Set<Integer> tagPairHashes = new HashSet<>();
			for (Map.Entry<String, String> tagPairEntry : tags.entries())
			{
				tagPairHashes.add(hashForTagPair(tagPairEntry.getKey(), tagPairEntry.getValue()));
			}
			if (tagPairHashes.isEmpty())
			{
				tagPairHashes.add(0);
			}
			List<Statement> queryStatements = new ArrayList<>(tagPairHashes.size());
			Date timestamp = new Date(rowKeyTimestamp);
			for (Integer tagPairHash : tagPairHashes)
			{
				queryStatements.add(
						clusterConnection.psTagIndexedRowKeyQuery.bind()
								.setString(0, metricName)
								.setTimestamp(1, timestamp)
								.setInt(2, tagPairHash));
			}
			return queryStatements;
		}

		private Set<Integer> generateTagPairHashes(DataPointsRowKey rowKey)
		{
			Set<Integer> tagPairHashes = new HashSet<>(rowKey.getTags().size() + 1);
			tagPairHashes.add(WILDCARD_TAG_HASH_VALUE);
			for (Map.Entry<String, String> tagPairEntry : rowKey.getTags().entrySet())
			{
				tagPairHashes.add(hashForTagPair(tagPairEntry.getKey(), tagPairEntry.getValue()));
			}
			return tagPairHashes;
		}

		private int hashForTagPair(String tagName, String tagValue)
		{
			return Objects.hash(tagName, tagValue);
		}
	}

}
