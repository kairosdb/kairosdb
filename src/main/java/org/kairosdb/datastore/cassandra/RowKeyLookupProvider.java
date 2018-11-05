package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
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
		public RowKeyResultSetProcessor createRowKeyQueryProcessor(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			return new RowKeyResultSetProcessor()
			{
				@Override
				public List<Statement> getQueryStatements()
				{
					return ImmutableList.of(
							clusterConnection.psRowKeyQuery.bind()
									.setString(0, metricName)
									.setTimestamp(1, new Date(rowKeyTimestamp)));
				}

				@Override
				public ResultSet apply(List<ResultSet> input)
				{
					if (input.size() != 1)
					{
						throw new IllegalStateException("Expected exactly 1 result set, got " + input);
					}
					return input.get(0);
				}
			};
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
			TagSetHash tagSetHash = generateTagPairHashes(rowKey);
			List<Statement> insertStatements = new ArrayList<>(tagSetHash.getTagPairHashes().size());
			Date rowKeyTimestamp = new Date(rowKey.getTimestamp());
			for (Integer tagPairHash : tagSetHash.getTagPairHashes())
			{
				insertStatements.add(
						clusterConnection.psTagIndexedRowKeyInsert.bind()
								.setString(0, rowKey.getMetricName())
								.setTimestamp(1, rowKeyTimestamp)
								.setString(2, rowKey.getDataType())
								.setInt(3, tagPairHash)
								.setInt(4, tagSetHash.getTagCollectionHash())
								.setMap(5, rowKey.getTags())
								.setInt(6, rowKeyTtl)
								.setIdempotent(true));
			}
			return insertStatements;
		}

		@Override
		public List<Statement> createDeleteStatements(DataPointsRowKey rowKey)
		{
			TagSetHash tagSetHash = generateTagPairHashes(rowKey);
			List<Statement> deleteStatements = new ArrayList<>(tagSetHash.getTagPairHashes().size());
			Date rowKeyTimestamp = new Date(rowKey.getTimestamp());
			for (Integer tagPairHash : tagSetHash.getTagPairHashes()) {
				deleteStatements.add(
						clusterConnection.psTagIndexedRowKeyDelete.bind()
								.setString(0, rowKey.getMetricName())
								.setTimestamp(1, rowKeyTimestamp)
								.setString(2, rowKey.getDataType())
								.setInt(3, tagPairHash)
								.setInt(4, tagSetHash.getTagCollectionHash())
								.setMap(5, rowKey.getTags()));
			}
			return deleteStatements;
		}

		@Override
		public RowKeyResultSetProcessor createRowKeyQueryProcessor(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{

			ListMultimap<String, Statement> queryStatementByTagName = createQueryStatementsByTagName(metricName, rowKeyTimestamp, tags);
			List<Statement> queryStatements = new ArrayList<>(queryStatementByTagName.size());
			Multimap<String, Integer> tagNameToStatementIndexes = ArrayListMultimap.create();

			for (Map.Entry<String, Statement> tagNameAndStatementEntry : queryStatementByTagName.entries())
			{
				String tagName = tagNameAndStatementEntry.getKey();
				Statement queryStatement = tagNameAndStatementEntry.getValue();
				tagNameToStatementIndexes.put(tagName, queryStatements.size());
				queryStatements.add(queryStatement);
			}

			return new RowKeyResultSetProcessor()
			{
				@Override
				public List<Statement> getQueryStatements()
				{
					return queryStatements;
				}

				@Override
				public ResultSet apply(List<ResultSet> input)
				{
					if (input.size() == 1)
					{
						// No need to find the most selective ResultSet if we've only got 1
						return input.get(0);
					}
					List<List<ResultSet>> resultSetsGroupedByTagName = new ArrayList<>(tagNameToStatementIndexes.size());
					for (Collection<Integer> indexCollection : tagNameToStatementIndexes.asMap().values())
					{
						List<ResultSet> resultSetsForTag = new ArrayList<>(indexCollection.size());
						for (Integer resultSetIndex : indexCollection)
						{
							resultSetsForTag.add(input.get(resultSetIndex));
						}
						resultSetsGroupedByTagName.add(resultSetsForTag);
					}
					Comparator<RowCountEstimatingRowKeyResultSet> comparator =
							Comparator
									.<RowCountEstimatingRowKeyResultSet>comparingInt(r -> r.isEstimated() ? 1 : 0)
									.thenComparing(RowCountEstimatingRowKeyResultSet::getRowCount);
					return resultSetsGroupedByTagName.stream().map(RowCountEstimatingRowKeyResultSet::create).min(comparator)
							.orElseThrow(() -> new IllegalStateException("No minimal ResultSet found"));
				}
			};
		}

		private ListMultimap<String, Statement> createQueryStatementsByTagName(String metricName, long rowKeyTimestamp, SetMultimap<String, String> tags)
		{
			// Using tag pair hashes as the key in this map can lead to collisions, but that's not a problem
			// because we're simply using the tag pair hashes as an initial AND filter, and further filtering is
			// done on the incoming ResultSets
			Map<Integer, String> tagPairHashToTagName = new HashMap<>();
			for (Map.Entry<String, String> tagPairEntry : tags.entries())
			{
				tagPairHashToTagName.put(
						hashForTagPair(tagPairEntry.getKey(), tagPairEntry.getValue()),
						tagPairEntry.getKey());
			}
			if (tagPairHashToTagName.isEmpty())
			{
				tagPairHashToTagName.put(WILDCARD_TAG_HASH_VALUE, "");
			}
			Date timestamp = new Date(rowKeyTimestamp);
			ListMultimap<String, Statement> queryStatementsByTagName = ArrayListMultimap.create(tagPairHashToTagName.size(), 1);
			for (Map.Entry<Integer, String> tagPairHashAndTagNameEntry : tagPairHashToTagName.entrySet())
			{
				int tagPairHash = tagPairHashAndTagNameEntry.getKey();
				String tagName = tagPairHashAndTagNameEntry.getValue();
				queryStatementsByTagName.put(
						tagName,
						clusterConnection.psTagIndexedRowKeyQuery.bind()
								.setString(0, metricName)
								.setTimestamp(1, timestamp)
								.setInt(2, tagPairHash));
			}
			return queryStatementsByTagName;
		}

		private TagSetHash generateTagPairHashes(DataPointsRowKey rowKey)
		{
			Hasher tagCollectionHasher = Hashing.murmur3_32().newHasher();
			Set<Integer> tagPairHashes = new HashSet<>(rowKey.getTags().size() + 1);
			tagPairHashes.add(WILDCARD_TAG_HASH_VALUE);
			for (Map.Entry<String, String> tagPairEntry : rowKey.getTags().entrySet())
			{
				int hashForTagPair = hashForTagPair(tagPairEntry.getKey(), tagPairEntry.getValue());
				tagPairHashes.add(hashForTagPair);
				tagCollectionHasher.putString(tagPairEntry.getKey(), Charsets.UTF_8);
				tagCollectionHasher.putString(tagPairEntry.getValue(), Charsets.UTF_8);
			}
			return new TagSetHash(tagCollectionHasher.hash().asInt(), tagPairHashes);
		}

		private int hashForTagPair(String tagName, String tagValue)
		{
			return Objects.hash(tagName, tagValue);
		}
	}

	/**
	 * Holds a hash of a full set of tag pairs, as well as individual tag pair hashes.
	 */
	private static class TagSetHash {

		private final int tagCollectionHash;
		private final Set<Integer> tagPairHashes;

		public TagSetHash(int tagCollectionHash, Set<Integer> tagPairHashes)
		{
			this.tagCollectionHash = tagCollectionHash;
			this.tagPairHashes = tagPairHashes;
		}

		public int getTagCollectionHash()
		{
			return tagCollectionHash;
		}

		public Set<Integer> getTagPairHashes()
		{
			return tagPairHashes;
		}
	}

}
