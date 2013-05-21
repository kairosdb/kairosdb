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
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.groupby.Grouper;
import org.kairosdb.core.groupby.TagGroupBy;
import org.kairosdb.core.groupby.TagGroupByResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class KairosDatastore
{
	public static final Logger logger = LoggerFactory.getLogger(KairosDatastore.class);


	private MessageDigest m_messageDigest;
	private String m_baseCacheDir;
	private volatile String m_cacheDir;
	private Datastore m_datastore;
	private List<DataPointListener> m_dataPointListeners;

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Inject
	public KairosDatastore(Datastore datastore, List<DataPointListener> dataPointListeners) throws DatastoreException
	{
		m_datastore = datastore;
		m_dataPointListeners = dataPointListeners;

		try
		{
			m_baseCacheDir = System.getProperty("java.io.tmpdir") + "/kairos_cache/";
			cleanDirectory(new File(m_baseCacheDir));
			newCacheDirectory();
			File cacheDirectory = new File(m_cacheDir);
			cacheDirectory.mkdirs();
			checkState(cacheDirectory.exists(), "Cache directory not created");
			m_messageDigest = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new DatastoreException(e);
		}
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

	public void cleanCacheDir()
	{
		String oldCacheDir = m_cacheDir;
		newCacheDirectory();

		try
		{
			Thread.sleep(60000);
		}
		catch (InterruptedException e)
		{
			logger.error("Sleep interrupted:", e);
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
	public List<DataPointRow> export(QueryMetric metric) throws DatastoreException
	{
		checkNotNull(metric);

		CachedSearchResult cachedResults = null;

		try
		{
			String cacheFilename = calculateFilenameHash(metric);
			String tempFile = m_cacheDir + cacheFilename;


			if (metric.getCacheTime() > 0)
			{
				cachedResults = CachedSearchResult.openCachedSearchResult(metric.getName(),
						tempFile, metric.getCacheTime());
			}

			if (cachedResults == null)
			{
				cachedResults = CachedSearchResult.createCachedSearchResult(metric.getName(), tempFile);
			}
		}
		catch (Exception e)
		{
			throw new DatastoreException(e);
		}

		return (m_datastore.queryDatabase(metric, cachedResults));
	}


	public List<DataPointGroup> query(QueryMetric metric) throws DatastoreException
	{
		checkNotNull(metric);

		CachedSearchResult cachedResults = null;

		List<DataPointRow> returnedRows = null;
		try
		{
			String cacheFilename = calculateFilenameHash(metric);
			String tempFile = m_cacheDir + cacheFilename;

			if (metric.getCacheTime() > 0)
			{
				cachedResults = CachedSearchResult.openCachedSearchResult(metric.getName(),
						tempFile, metric.getCacheTime());
				if (cachedResults != null)
				{
					returnedRows = cachedResults.getRows();
					logger.debug("Cache HIT!");
				}
			}

			if (cachedResults == null)
			{
				logger.debug("Cache MISS!");
				cachedResults = CachedSearchResult.createCachedSearchResult(metric.getName(),
						tempFile);
				returnedRows = m_datastore.queryDatabase(metric, cachedResults);
			}
		}
		catch (Exception e)
		{
			throw new DatastoreException(e);
		}

		// It is more efficient to group by tags using the cached results because we have pointers to each tag.
		List<DataPointGroup> queryResults = groupByTags(wrapRows(returnedRows), getTagGroupBy(metric.getGroupBys()));

		// Now group for all other types of group bys.
		Grouper grouper = new Grouper();
		try
		{
			queryResults = grouper.group(removeTagGroupBy(metric.getGroupBys()), queryResults);
		}
		catch (IOException e)
		{
			throw new DatastoreException(e);
		}

		List<DataPointGroup> aggregatedResults = new ArrayList<DataPointGroup>();
		for (DataPointGroup queryResult : queryResults)
		{
			DataPointGroup aggregatedGroup = queryResult;

			List<Aggregator> aggregators = metric.getAggregators();

			//This will pipe the aggregators together.
			for (Aggregator aggregator : aggregators)
			{
				aggregatedGroup = aggregator.aggregate(aggregatedGroup);
			}

			aggregatedResults.add(aggregatedGroup);
		}

		return aggregatedResults;
	}

	private List<GroupBy> removeTagGroupBy(List<GroupBy> groupBys)
	{
		List<GroupBy> modifiedGroupBys = new ArrayList<GroupBy>();
		for (GroupBy groupBy : groupBys)
		{
			if (!(groupBy instanceof TagGroupBy))
				modifiedGroupBys.add(groupBy);
		}
		return modifiedGroupBys;
	}

	private TagGroupBy getTagGroupBy(List<GroupBy> groupBys)
	{
		for (GroupBy groupBy : groupBys)
		{
			if (groupBy instanceof TagGroupBy)
				return (TagGroupBy) groupBy;
		}
		return null;
	}

	private List<DataPointGroup> wrapRows(List<DataPointRow> rows)
	{
		List<DataPointGroup> ret = new ArrayList<DataPointGroup>();

		for (DataPointRow row : rows)
		{
			ret.add(new DataPointGroupRowWrapper(row));
		}

		return (ret);
	}

	private List<DataPointGroup> groupByTags(List<DataPointGroup> dataPointsList, TagGroupBy tagGroupBy)
	{
		List<DataPointGroup> ret = new ArrayList<DataPointGroup>();

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
			}

			for (String key : groups.keySet())
			{
				ret.add(new SortingDataPointGroup(groups.get(key), groupByResults.get(key)));
			}
		}
		else
		{
			ret.add(new SortingDataPointGroup(dataPointsList));
		}

		return ret;
	}

	private String getTagsKey(LinkedHashMap<String, String> tags)
	{
		StringBuilder builder = new StringBuilder();
		for (String name : tags.keySet())
		{
			builder.append(tags.get(name));
		}

		return builder.toString();
	}

	private LinkedHashMap<String, String> getMatchingTags(DataPointGroup datapointGroup, List<String> tagNames)
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


	private String calculateFilenameHash(QueryMetric metric) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{
		String hashString = metric.getCacheString();
		if (hashString == null)
			hashString = String.valueOf(System.currentTimeMillis());

		byte[] digest = m_messageDigest.digest(hashString.getBytes("UTF-8"));

		return new BigInteger(1, digest).toString(16);
	}
}
