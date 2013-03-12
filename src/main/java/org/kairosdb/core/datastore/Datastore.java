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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.TournamentTree;
import org.kairosdb.core.aggregator.Aggregator;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Datastore
{
	MessageDigest messageDigest;

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
	 Exports the data for a metric query without doing any aggregation or sorting
	 @param metric
	 @return
	 @throws DatastoreException
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

		List<DataPointGroup> aggregatedResults = new ArrayList<DataPointGroup>();

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

		List<DataPointGroup> aggregatedResults = new ArrayList<DataPointGroup>();

		List<List<DataPointGroup>> queryResults = groupBy(metric.getName(),
				wrapRows(queryDatabase(metric, cachedResults)), metric.getGroupBy());

		for (List<DataPointGroup> dataPointGroupList : queryResults)
		{
			List<DataPointGroup> aggregatedGroupList = dataPointGroupList;

			//This will pipe the aggregators together.
			for (Aggregator aggregator : metric.getAggregators())
			{
				aggregatedGroupList = Collections.singletonList(aggregator.createAggregatorGroup(aggregatedGroupList));
			}

			//Take whatever is left and add them to the return list.
			//If there were no aggregation done then we iterate over dataPointGroupList
			//and return what we got from the data store
			for (DataPointGroup dataPointGroup : aggregatedGroupList)
			{
				aggregatedResults.add(dataPointGroup);
			}

			/*Aggregator aggregator = aggregators.get(metric.getAggregator());
			if (aggregator == null)
			{
				throw new UnknownAggregator(metric.getAggregator());
			}

			aggregatedResults.add(aggregator.aggregate(dataPointGroupList));*/
		}

		return aggregatedResults;
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

	private List<List<DataPointGroup>> groupBy(String metricName, List<DataPointGroup> dataPointsList, String groupByTag)
	{
		List<List<DataPointGroup>> ret = new ArrayList<List<DataPointGroup>>();

		if (groupByTag != null)
		{
			ListMultimap<String, DataPointGroup> groups = ArrayListMultimap.create();
			//Map<String, List<DataPointGroup>> groups = new HashMap<String, List<DataPointGroup>>();

			for (DataPointGroup dataPointGroup : dataPointsList)
			{
				//Todo: Add code to datastore implementations to filter by the group by tag

				Set<String> tagValues = dataPointGroup.getTagValues(groupByTag);
				if (tagValues == null)
					continue;

				String tagValue = tagValues.iterator().next();

				if (tagValue == null)
					continue;

				groups.put(tagValue, dataPointGroup);
				/*TournamentTreeDataGroup tree = groups.get(tagValue);
				if (tree == null)
				{
					tree = new TournamentTreeDataGroup(metricName);
					groups.put(tagValue, tree);
				}

				tree.addIterator(new DataPointGroupRowWrapper(dataPointGroup));*/
			}

			for (String key : groups.keySet())
			{
				ret.add(groups.get(key));
			}
		}
		else
		{
			ret.add(dataPointsList);
		}

		return ret;
	}

	protected abstract List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException;

	private class TournamentTreeDataGroup extends AbstractDataPointGroup
	{
		private TournamentTree<DataPoint> tree;
		//We keep this list so we can close the iterators
		private List<DataPointGroup> taggedDataPointsList = new ArrayList<DataPointGroup>();

		public TournamentTreeDataGroup(String name)
		{
			super(name);

			tree = new TournamentTree<DataPoint>(new DataPointComparator());
		}

		public TournamentTreeDataGroup(String name, List<DataPointRow> listDataPointRow)
		{
			this(name);

			for (DataPointRow dataPoints : listDataPointRow)
			{
				addIterator(new DataPointGroupRowWrapper(dataPoints));
			}
		}

		/*public TournamentTreeDataGroup(String name, List<DataPointGroup> listDataPointGroup)
		{
			this(name);

			for (DataPointGroup dataPoints : listDataPointGroup)
			{
				addIterator(dataPoints);
			}
		}*/

		@Override
		public void close()
		{
			for (DataPointGroup taggedDataPoints : taggedDataPointsList)
			{
				taggedDataPoints.close();
			}
		}

		@Override
		public boolean hasNext()
		{
			return tree.hasNext();
		}

		@Override
		public DataPoint next()
		{
			DataPoint ret = tree.nextElement();

			return ret;
		}

		public void addIterator(DataPointGroup taggedDataPoints)
		{
			tree.addIterator(taggedDataPoints);
			addTags(taggedDataPoints);
			taggedDataPointsList.add(taggedDataPoints);
		}
	}

	private class DataPointComparator implements Comparator<DataPoint>
	{
		@Override
		public int compare(DataPoint point1, DataPoint point2)
		{
			return point1.compareTo(point2);
		}
	}

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
