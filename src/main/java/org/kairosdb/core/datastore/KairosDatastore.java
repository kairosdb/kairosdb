/*
 * Copyright 2013 Proofpoint Inc.
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
import com.google.inject.name.Named;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.LimitAggregator;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.*;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class KairosDatastore
{
	public static final Logger logger = LoggerFactory.getLogger(KairosDatastore.class);
	public static final String QUERY_CACHE_DIR = "kairosdb.query_cache.cache_dir";
	public static final String QUERY_METRIC_TIME = "kairosdb.datastore.query_time";
	public static final String QUERIES_WAITING_METRIC_NAME = "kairosdb.datastore.queries_waiting";

	private final Datastore m_datastore;
	private final QueryQueuingManager m_queuingManager;
	private final List<DataPointListener> m_dataPointListeners;
	private final String m_hostname;
	private final KairosDataPointFactory m_dataPointFactory;

	private String m_baseCacheDir;
	private volatile String m_cacheDir;

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Inject
	public KairosDatastore(Datastore datastore, QueryQueuingManager queuingManager,
	      List<DataPointListener> dataPointListeners, @Named("HOSTNAME") String hostname,
			KairosDataPointFactory dataPointFactory)
			throws DatastoreException
	{
		m_datastore = checkNotNull(datastore);
		m_dataPointListeners = checkNotNull(dataPointListeners);
		m_queuingManager = checkNotNull(queuingManager);
		m_hostname = checkNotNullOrEmpty(hostname);
		m_dataPointFactory = dataPointFactory;

		m_baseCacheDir = System.getProperty("java.io.tmpdir") + "/kairos_cache/";

		cleanDirectory(new File(m_baseCacheDir));
		newCacheDirectory();
		File cacheDirectory = new File(m_cacheDir);
		cacheDirectory.mkdirs();
		checkState(cacheDirectory.exists(), "Cache directory not created");
	}

	@Inject(optional = true)
	public void setBaseCacheDir(@Named(QUERY_CACHE_DIR) String cacheTempDir)
	{
		if (cacheTempDir != null && !cacheTempDir.equals(""))
			m_baseCacheDir = cacheTempDir;
	}

	public String getCacheDir()
	{
		return (m_cacheDir);
	}

	private void newCacheDirectory()
	{
		m_cacheDir = m_baseCacheDir + "/" + System.currentTimeMillis() + "/";
		File cacheDirectory = new File(m_cacheDir);
		cacheDirectory.mkdirs();
	}

	private void cleanDirectory(File directory)
	{
		if (!directory.exists())
			return;
		File[] list = directory.listFiles();

		if (list.length > 0)
		{
			for (File aList : list)
			{
				if (aList.isDirectory())
					cleanDirectory(aList);

				aList.delete();
			}
		}

		directory.delete();
	}

	public void cleanCacheDir(boolean wait)
	{
		String oldCacheDir = m_cacheDir;
		newCacheDirectory();

		if (wait)
		{
			try
			{
				Thread.sleep(60000);
			}
			catch (InterruptedException e)
			{
				logger.error("Sleep interrupted:", e);
			}
		}

		logger.debug("Executing job...");
		File dir = new File(oldCacheDir);
		logger.debug("Deleting cache files in " + dir.getAbsolutePath());

		cleanDirectory(dir);
	}

	/**
	 * Close the datastore
	 */
	public void close() throws InterruptedException, DatastoreException
	{
		m_datastore.close();
	}

	public void putDataPoints(DataPointSet dps) throws DatastoreException
	{
		//Add to datastore first.
		m_datastore.putDataPoints(dps);

		for (DataPointListener dataPointListener : m_dataPointListeners)
		{
			dataPointListener.dataPoints(dps);
		}
	}

	public Iterable<String> getMetricNames() throws DatastoreException
	{
		return (m_datastore.getMetricNames());
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
	 * @return list of data point rows
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
		checkNotNull(metric);

		DatastoreQuery dq;

		try
		{
			dq = new DatastoreQueryImpl(metric);
		}
		catch (UnsupportedEncodingException e)
		{
			throw new DatastoreException(e);
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new DatastoreException(e);
		}
		catch (InterruptedException e)
		{
			throw new DatastoreException(e);
		}

		return (dq);
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

	private static List<DataPointGroup> wrapRows(List<DataPointRow> rows)
	{
		List<DataPointGroup> ret = new ArrayList<DataPointGroup>();

		MemoryMonitor mm = new MemoryMonitor(100);
		for (DataPointRow row : rows)
		{
			ret.add(new DataPointGroupRowWrapper(row));
			mm.checkMemoryAndThrowException();
		}

		return (ret);
	}

	private List<DataPointGroup> groupByTypeAndTag(String metricName,
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

			for (DataPointRow row : rows)
			{
				String groupType = m_dataPointFactory.getGroupType(row.getDatastoreType());

				typeGroups.put(groupType, new DataPointGroupRowWrapper(row));
				mm.checkMemoryAndThrowException();
			}

			for (String type : typeGroups.keySet())
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

					for (String key : groups.keySet())
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


	private static List<DataPointGroup> groupByTags(String metricName,
			List<DataPointGroup> dataPointsList, TagGroupBy tagGroupBy, Order order)
	{
		List<DataPointGroup> ret = new ArrayList<DataPointGroup>();
		MemoryMonitor mm = new MemoryMonitor(20);

		if (dataPointsList.isEmpty())
		{
			ret.add(new SortingDataPointGroup(metricName, order));
		}
		else
		{
			if (tagGroupBy != null)
			{
				ListMultimap<String, DataPointGroup> groups = ArrayListMultimap.create();
				Map<String, TagGroupByResult> groupByResults = new HashMap<String, TagGroupByResult>();

				for (DataPointGroup dataPointGroup : dataPointsList)
				{
					//Todo: Add code to datastore implementations to filter by the group by tag

					LinkedHashMap<String, String> matchingTags = getMatchingTags(dataPointGroup, tagGroupBy.getTagNames());
					String tagsKey = getTagsKey(matchingTags);
					groups.put(tagsKey, dataPointGroup);
					groupByResults.put(tagsKey, new TagGroupByResult(tagGroupBy, matchingTags));
					mm.checkMemoryAndThrowException();
				}

				for (String key : groups.keySet())
				{
					ret.add(new SortingDataPointGroup(groups.get(key), groupByResults.get(key), order));
				}
			}
			else
			{
				ret.add(new SortingDataPointGroup(dataPointsList, order));
			}
		}

		return ret;
	}

	private static String getTagsKey(LinkedHashMap<String, String> tags)
	{
		StringBuilder builder = new StringBuilder();
		for (String name : tags.keySet())
		{
			builder.append(tags.get(name));
		}

		return builder.toString();
	}

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


	private static String calculateFilenameHash(QueryMetric metric) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{
		String hashString = metric.getCacheString();
		if (hashString == null)
			hashString = String.valueOf(System.currentTimeMillis());

		MessageDigest messageDigest = MessageDigest.getInstance("MD5");
		byte[] digest = messageDigest.digest(hashString.getBytes("UTF-8"));

		return new BigInteger(1, digest).toString(16);
	}


	private class DatastoreQueryImpl implements DatastoreQuery
	{
		private String m_cacheFilename;
		private QueryMetric m_metric;
		private List<DataPointGroup> m_results;
		private int m_dataPointCount;
		
		public DatastoreQueryImpl(QueryMetric metric)
				throws UnsupportedEncodingException, NoSuchAlgorithmException,
				InterruptedException, DatastoreException
		{
			//Report number of queries waiting
			int waitingCount = m_queuingManager.getQueryWaitingCount();
			if (waitingCount != 0)
			{
				ThreadReporter.addDataPoint(QUERIES_WAITING_METRIC_NAME, waitingCount);
			}

			m_metric = metric;
			m_cacheFilename = calculateFilenameHash(metric);
			m_queuingManager.waitForTimeToRun(m_cacheFilename);
		}

		public int getSampleSize()
		{
			return m_dataPointCount;
		}

		@Override
		public List<DataPointGroup> execute() throws DatastoreException
		{
			long queryStartTime = System.currentTimeMillis();
			
			CachedSearchResult cachedResults = null;

			List<DataPointRow> returnedRows = null;

			try
			{
				String tempFile = m_cacheDir + m_cacheFilename;

				if (m_metric.getCacheTime() > 0)
				{
					cachedResults = CachedSearchResult.openCachedSearchResult(m_metric.getName(),
							tempFile, m_metric.getCacheTime(), m_dataPointFactory);
					if (cachedResults != null)
					{
						returnedRows = cachedResults.getRows();
						logger.debug("Cache HIT!");
					}
				}

				if (cachedResults == null)
				{
					logger.debug("Cache MISS!");
					cachedResults = CachedSearchResult.createCachedSearchResult(m_metric.getName(),
							tempFile, m_dataPointFactory);
					m_datastore.queryDatabase(m_metric, cachedResults);
					returnedRows = cachedResults.getRows();
				}
			}
			catch (Exception e)
			{
				throw new DatastoreException(e);
			}

			//Get data point count
			for (DataPointRow returnedRow : returnedRows)
			{
				m_dataPointCount += returnedRow.getDataPointCount();
			}

			List<DataPointGroup> queryResults = groupByTypeAndTag(m_metric.getName(),
					returnedRows, getTagGroupBy(m_metric.getGroupBys()), m_metric.getOrder());

			/*if (m_metric.getGroupBys() != null)
			{
				// It is more efficient to group by tags using the cached results because we have pointers to each tag.
				queryResults = groupByTags(m_metric.getName(),
						queryResults, getTagGroupBy(m_metric.getGroupBys()), m_metric.getOrder());
			}*/

			// Now group for all other types of group bys.
			Grouper grouper = new Grouper();
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
				DataPointGroup aggregatedGroup = queryResult;

				List<Aggregator> aggregators = m_metric.getAggregators();

				if (m_metric.getLimit() != 0)
				{
					aggregatedGroup = new LimitAggregator(m_metric.getLimit()).aggregate(aggregatedGroup);
				}

				//This will pipe the aggregators together.
				for (Aggregator aggregator : aggregators)
				{
					aggregatedGroup = aggregator.aggregate(aggregatedGroup);
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
				m_queuingManager.done(m_cacheFilename);
			}
		}
	}
}
