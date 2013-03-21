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
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.groupby.Grouper;
import org.kairosdb.core.groupby.TagGroupBy;
import org.kairosdb.core.groupby.TagGroupByResult;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Datastore
{
	private MessageDigest messageDigest;

	protected Datastore() throws DatastoreException
	{
		try
		{
			messageDigest = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new DatastoreException(e);
		}
	}

	/**
	 * Close the datastore
	 */
	public abstract void close() throws InterruptedException, DatastoreException;

	public abstract void putDataPoints(DataPointSet dps) throws DatastoreException;

	public abstract Iterable<String> getMetricNames() throws DatastoreException;

	public abstract Iterable<String> getTagNames() throws DatastoreException;

	public abstract Iterable<String> getTagValues() throws DatastoreException;

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
			String tempFile = System.getProperty("java.io.tmpdir") + "/" + cacheFilename;

			if (metric.getCacheTime() > 0)
			{
				cachedResults = CachedSearchResult.openCachedSearchResult(metric.getName(), tempFile, metric.getCacheTime());
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

		return (queryDatabase(metric, cachedResults));
	}


	public List<DataPointGroup> query(QueryMetric metric) throws DatastoreException
	{
		checkNotNull(metric);

		CachedSearchResult cachedResults = null;

		try
		{
			String cacheFilename = calculateFilenameHash(metric);
			String tempFile = System.getProperty("java.io.tmpdir") + "/" + cacheFilename;

			if (metric.getCacheTime() > 0)
			{
				cachedResults = CachedSearchResult.openCachedSearchResult(metric.getName(), tempFile, metric.getCacheTime());
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

		// It is more efficient to group by tags using the cached results because we have pointers to each tag.
		List<DataPointGroup> queryResults = groupByTags(wrapRows(queryDatabase(metric, cachedResults)), getTagGroupBy(metric.getGroupBys()));

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

	protected abstract List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException;

	private String calculateFilenameHash(QueryMetric metric) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{
		StringBuilder builder = new StringBuilder();
		builder.append(metric.getName());
		builder.append(metric.getStartTime());
		builder.append(metric.getEndTime());

		SortedMap<String, String> tags = metric.getTags();
		for (String key : metric.getTags().keySet())
		{
			builder.append(key).append("=").append(tags.get(key));
		}

		byte[] digest = messageDigest.digest(builder.toString().getBytes("UTF-8"));

		return new BigInteger(1, digest).toString(16);
	}
}
