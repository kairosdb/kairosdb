/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.datastore;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.aggregator.LimitAggregator;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.core.groupby.Grouper;
import org.kairosdb.core.groupby.TagGroupBy;
import org.kairosdb.core.groupby.TagGroupByResult;
import org.kairosdb.core.groupby.TypeGroupByResult;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class KairosDatastore
{
	public static final Logger logger = LoggerFactory.getLogger(KairosDatastore.class);

	public static final String QUERY_METRIC_TIME = "kairosdb.datastore.query_time";
	public static final String QUERIES_WAITING_METRIC_NAME = "kairosdb.datastore.queries_waiting";
	public static final String QUERY_SAMPLE_SIZE = "kairosdb.datastore.query_sample_size";
	public static final String QUERY_ROW_COUNT = "kairosdb.datastore.query_row_count";

	private final Datastore m_datastore;
	private final QueryQueuingManager m_queuingManager;
	private final KairosDataPointFactory m_dataPointFactory;
	private final SearchResultFactory m_searchResultFactory;

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Inject
	public KairosDatastore(Datastore datastore, QueryQueuingManager queuingManager,
			KairosDataPointFactory dataPointFactory,
			SearchResultFactory searchResultFactory)
	{
		m_datastore = checkNotNull(datastore);
		m_queuingManager = checkNotNull(queuingManager);
		m_dataPointFactory = dataPointFactory;
		m_searchResultFactory = searchResultFactory;
	}

	public Datastore getDatastore()
	{
		return m_datastore;
	}

	/**
	 * Close the datastore
	 */
	public void close() throws InterruptedException, DatastoreException
	{
		m_datastore.close();
	}

	/*public void putDataPoint(String metricName,
			ImmutableSortedMap<String, String> tags,
			DataPoint dataPoint) throws DatastoreException
	{
		putDataPoint(metricName, tags, dataPoint, 0);
	}

	public void putDataPoint(String metricName,
			ImmutableSortedMap<String, String> tags,
			DataPoint dataPoint, int ttl) throws DatastoreException
	{
		//Add to datastore first.
		m_datastore.putDataPoint(metricName, tags, dataPoint, ttl);
	}*/


	public Iterable<String> getMetricNames(String prefix) throws DatastoreException
	{
		return (m_datastore.getMetricNames(prefix));
	}

	public Iterable<String> getTagNames() throws DatastoreException
	{
		return (m_datastore.getTagNames());
	}

	public Iterable<String> getTagValues() throws DatastoreException
	{
		return (m_datastore.getTagValues());
	}

	/**
	 * Exports the data for a metric query without doing any aggregation or sorting
	 *
	 * @param metric metric
	 * @throws DatastoreException
	 */
	public void export(QueryMetric metric, QueryCallback callback) throws DatastoreException
	{
		checkNotNull(metric);

		m_datastore.queryDatabase(metric, callback);
	}


	public List<DataPointGroup> queryTags(QueryMetric metric) throws DatastoreException
	{
		TagSet tagSet = m_datastore.queryMetricTags(metric);

		return Collections.<DataPointGroup>singletonList(new EmptyDataPointGroup(metric.getName(), tagSet));

	}

	public DatastoreQuery createQuery(QueryMetric metric) throws DatastoreException
	{
		return createQuery(metric, DatastoreQueryContext.createAtomic());
	}

	public DatastoreQuery createQuery(QueryMetric metric, DatastoreQueryContext datastoreQueryContext) throws DatastoreException
	{
		checkNotNull(metric);

		try
		{
			return new DatastoreQueryImpl(metric, datastoreQueryContext);
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			throw new DatastoreException(e);
		}
	}


	public void delete(QueryMetric metric) throws DatastoreException
	{
		checkNotNull(metric);

		try
		{
			m_datastore.deleteDataPoints(metric);
		}
		catch (Exception e)
		{
			throw new DatastoreException(e);
		}
	}

	private static List<GroupBy> removeTagGroupBy(List<GroupBy> groupBys)
	{
		List<GroupBy> modifiedGroupBys = new ArrayList<GroupBy>();
		for (GroupBy groupBy : groupBys)
		{
			if (!(groupBy instanceof TagGroupBy))
				modifiedGroupBys.add(groupBy);
		}
		return modifiedGroupBys;
	}

	private static TagGroupBy getTagGroupBy(List<GroupBy> groupBys)
	{
		for (GroupBy groupBy : groupBys)
		{
			if (groupBy instanceof TagGroupBy)
				return (TagGroupBy) groupBy;
		}
		return null;
	}

	protected List<DataPointGroup> groupByTypeAndTag(String metricName,
			List<DataPointRow> rows, TagGroupBy tagGroupBy, Order order)
	{
		List<DataPointGroup> ret = new ArrayList<DataPointGroup>();
		MemoryMonitor mm = new MemoryMonitor(20);

		if (rows.isEmpty())
		{
			ret.add(new SortingDataPointGroup(metricName, order));
		}
		else
		{
			ListMultimap<String, DataPointGroup> typeGroups = ArrayListMultimap.create();

			//Go through each row grouping them by type
			for (DataPointRow row : rows)
			{
				String groupType = m_dataPointFactory.getGroupType(row.getDatastoreType());

				typeGroups.put(groupType, new DataPointGroupRowWrapper(row));
				mm.checkMemoryAndThrowException();
			}

			//Sort the types for predictable results
			TreeSet<String> sortedTypes = new TreeSet<String>(typeGroups.keySet());

			//Now go through each type group and group by tag if needed.
			for (String type : sortedTypes)
			{
				if (tagGroupBy != null)
				{
					ListMultimap<String, DataPointGroup> groups = ArrayListMultimap.create();
					Map<String, TagGroupByResult> groupByResults = new HashMap<String, TagGroupByResult>();

					for (DataPointGroup dataPointGroup : typeGroups.get(type))
					{
						//Todo: Add code to datastore implementations to filter by the group by tag

						LinkedHashMap<String, String> matchingTags = getMatchingTags(dataPointGroup, tagGroupBy.getTagNames());
						String tagsKey = getTagsKey(matchingTags);
						groups.put(tagsKey, dataPointGroup);
						groupByResults.put(tagsKey, new TagGroupByResult(tagGroupBy, matchingTags));
						mm.checkMemoryAndThrowException();
					}

					//Sort groups by tags
					TreeSet<String> sortedGroups = new TreeSet<String>(groups.keySet());

					for (String key : sortedGroups)
					{
						SortingDataPointGroup sdpGroup = new SortingDataPointGroup(groups.get(key), groupByResults.get(key), order);
						sdpGroup.addGroupByResult(new TypeGroupByResult(type));
						ret.add(sdpGroup);
					}
				}
				else
				{
					ret.add(new SortingDataPointGroup(typeGroups.get(type), new TypeGroupByResult(type), order));
				}
			}
		}

		return ret;
	}


	/**
	 * Create a unique identifier for this combination of tags to be used as the key of a hash map.
	 */
	private static String getTagsKey(LinkedHashMap<String, String> tags)
	{
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String,String> entry : tags.entrySet())
		{
			builder.append(entry.getKey()).append(entry.getValue());
		}

		return builder.toString();
	}

	/**
	 Tags are inserted in the order specified in tagNames which is the order
	 from the query.  We use a linked hashmap so that order is preserved and
	 the group by responses are sorted in the order specified in the query.
	 @param datapointGroup
	 @param tagNames
	 @return
	 */
	private static LinkedHashMap<String, String> getMatchingTags(DataPointGroup datapointGroup, List<String> tagNames)
	{
		LinkedHashMap<String, String> matchingTags = new LinkedHashMap<String, String>();
		for (String tagName : tagNames)
		{
			Set<String> tagValues = datapointGroup.getTagValues(tagName);
			if (tagValues != null)
			{
				String tagValue = tagValues.iterator().next();
				matchingTags.put(tagName, tagValue != null ? tagValue : "");
			}
		}

		return matchingTags;
	}

	private class DatastoreQueryImpl implements DatastoreQuery
	{
		private QueryMetric m_metric;
		private final DatastoreQueryContext m_datastoreQueryContext;
		private List<DataPointGroup> m_results;
		private int m_dataPointCount;
		private int m_rowCount;
		
		public DatastoreQueryImpl(QueryMetric metric, DatastoreQueryContext datastoreQueryContext) throws InterruptedException
		{
			//Report number of queries waiting
			int waitingCount = m_queuingManager.getQueryWaitingCount();
			if (waitingCount != 0)
			{
				ThreadReporter.addDataPoint(QUERIES_WAITING_METRIC_NAME, waitingCount);
			}

			m_metric = metric;
			m_datastoreQueryContext = datastoreQueryContext;
			m_queuingManager.waitForTimeToRun(metric.getCacheString());
		}

		public int getSampleSize()
		{
			return m_dataPointCount;
		}
		public int getRowCount() { return m_rowCount; }

		@Override
		public List<DataPointGroup> execute() throws DatastoreException
		{
			long queryStartTime = System.currentTimeMillis();
			
			SearchResult searchResult = null;

			List<DataPointRow> returnedRows = null;

			try
			{
				searchResult = m_searchResultFactory.createSearchResult(m_metric, m_datastoreQueryContext);
				returnedRows = searchResult.getRows();
			}
			catch (Exception e)
			{
				logger.error("Query Error", e);
				throw new DatastoreException(e);
			}

			//Get data point count
			for (DataPointRow returnedRow : returnedRows)
			{
				m_dataPointCount += returnedRow.getDataPointCount();
			}

			m_rowCount = returnedRows.size();

			ThreadReporter.addDataPoint(QUERY_SAMPLE_SIZE, m_dataPointCount);
			ThreadReporter.addDataPoint(QUERY_ROW_COUNT, m_rowCount);

			List<DataPointGroup> queryResults = groupByTypeAndTag(m_metric.getName(),
					returnedRows, getTagGroupBy(m_metric.getGroupBys()), m_metric.getOrder());


			// Now group for all other types of group bys.
			Grouper grouper = new Grouper(m_dataPointFactory);
			try
			{
				queryResults = grouper.group(removeTagGroupBy(m_metric.getGroupBys()), queryResults);
			}
			catch (IOException e)
			{
				throw new DatastoreException(e);
			}

			m_results = new ArrayList<DataPointGroup>();
			for (DataPointGroup queryResult : queryResults)
			{
				String groupType = DataPoint.GROUP_NUMBER;
				//todo May want to make group type a first class citizen in DataPointGroup
				for (GroupByResult groupByResult : queryResult.getGroupByResult())
				{
					if (groupByResult instanceof TypeGroupByResult)
					{
						groupType = ((TypeGroupByResult)groupByResult).getType();
					}
				}

				DataPointGroup aggregatedGroup = queryResult;

				List<Aggregator> aggregators = m_metric.getAggregators();

				if (m_metric.getLimit() != 0)
				{
					aggregatedGroup = new LimitAggregator(m_metric.getLimit()).aggregate(aggregatedGroup);
				}

				//This will pipe the aggregators together.
				for (Aggregator aggregator : aggregators)
				{
					//Make sure the aggregator can handle this type of data.
					if (aggregator.canAggregate(groupType)) {
						aggregatedGroup = aggregator.aggregate(aggregatedGroup);
						groupType = aggregator.getAggregatedGroupType(groupType);
					}
				}

				m_results.add(aggregatedGroup);
			}


			//Report how long query took
			ThreadReporter.addDataPoint(QUERY_METRIC_TIME, System.currentTimeMillis() - queryStartTime);

			return (m_results);
		}

		@Override
		public void close()
		{
			try
			{
				if (m_results != null)
				{
					for (DataPointGroup result : m_results)
					{
						result.close();
					}
				}
			}
			finally
			{  //This must get done
				m_queuingManager.done(m_metric.getCacheString());
				if (m_datastoreQueryContext.isAtomic()) {
					m_datastoreQueryContext.close();
				}
			}
		}
	}
}
