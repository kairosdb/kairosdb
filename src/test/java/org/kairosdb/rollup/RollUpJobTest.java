package org.kairosdb.rollup;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.Subscribe;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.core.aggregator.DiffAggregator;
import org.kairosdb.core.aggregator.DivideAggregator;
import org.kairosdb.core.aggregator.MaxAggregator;
import org.kairosdb.core.aggregator.MinAggregator;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.testing.ListDataPointGroup;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class RollUpJobTest
{
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss.SS",Locale.ENGLISH);

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private long lastTimeStamp;
	private KairosDatastore datastore;
	private TestDatastore testDataStore;

	@Before
	public void setup() throws ParseException, DatastoreException
	{
		lastTimeStamp = dateFormat.parse("2013-JAN-18 4:55:12.22").getTime();

		testDataStore = new TestDatastore();
		datastore = new KairosDatastore(testDataStore, new QueryQueuingManager(1, "hostname"),
				new TestDataPointFactory(), false);
	}

	@Test
	public void test_getLastSampling()
	{
		Sampling sampling1 = new Sampling(1, TimeUnit.DAYS);
		Sampling sampling2 = new Sampling(2, TimeUnit.MINUTES);

		DoubleDataPointFactory dataPointFactory = mock(DoubleDataPointFactory.class);

		MinAggregator minAggregator = new MinAggregator(dataPointFactory);
		minAggregator.setSampling(sampling1);
		MaxAggregator maxAggregator = new MaxAggregator(dataPointFactory);
		maxAggregator.setSampling(sampling2);

		List<Aggregator> aggregators = new ArrayList<Aggregator>();
		aggregators.add(minAggregator);
		aggregators.add(maxAggregator);
		aggregators.add(new DivideAggregator(dataPointFactory));
		aggregators.add(new DiffAggregator(dataPointFactory));

		Sampling lastSampling = RollUpJob.getLastSampling(aggregators);

		assertThat(lastSampling, equalTo(sampling2));

		aggregators = new ArrayList<Aggregator>();
		aggregators.add(maxAggregator);
		aggregators.add(new DivideAggregator(dataPointFactory));
		aggregators.add(new DiffAggregator(dataPointFactory));
		aggregators.add(minAggregator);

		lastSampling = RollUpJob.getLastSampling(aggregators);

		assertThat(lastSampling, equalTo(sampling1));
	}

	@Test
	public void test_getLastSampling_no_sampling()
	{
		DoubleDataPointFactory dataPointFactory = mock(DoubleDataPointFactory.class);
		List<Aggregator> aggregators = new ArrayList<Aggregator>();
		aggregators.add(new DivideAggregator(dataPointFactory));
		aggregators.add(new DiffAggregator(dataPointFactory));

		Sampling lastSampling = RollUpJob.getLastSampling(aggregators);

		assertThat(lastSampling, equalTo(null));
	}

	@Test
	public void test_getLastRollupDataPoint() throws ParseException, DatastoreException
	{
		long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
		String metricName = "foo";

		ImmutableSortedMap<String, String> localHostTags = ImmutableSortedMap.of("host", "localhost");
		List<DataPoint> localhostDataPoints = new ArrayList<DataPoint>();
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 3, 12));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 4, 13));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 5, 14));

		ImmutableSortedMap<String, String> remoteTags = ImmutableSortedMap.of("host", "remote");
		List<DataPoint> remoteDataPoints = new ArrayList<DataPoint>();
		remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
		remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));

		testDataStore.clear();
		testDataStore.putDataPoints(metricName, localHostTags, localhostDataPoints);
		testDataStore.putDataPoints(metricName, remoteTags, remoteDataPoints);

		DataPoint lastDataPoint = RollUpJob.getLastRollupDataPoint(datastore, metricName, now);

		// Look back from now and find last data point [4]
		assertThat(lastDataPoint, equalTo(localhostDataPoints.get(4)));
	}

	@Test
	public void test_getLastRollupDataPoint_noDataPoints() throws ParseException, DatastoreException
	{
		long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
		String metricName = "foo";

		testDataStore.clear();

		DataPoint lastDataPoint = RollUpJob.getLastRollupDataPoint(datastore, metricName, now);

		assertThat(lastDataPoint, equalTo(null));
	}

	@Test
	public void test_getgetFutureDataPoint() throws ParseException, DatastoreException
	{
		long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
		String metricName = "foo";

		ImmutableSortedMap<String, String> localHostTags = ImmutableSortedMap.of("host", "localhost");
		List<DataPoint> localhostDataPoints = new ArrayList<DataPoint>();
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 3, 12));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 4, 13));
		localhostDataPoints.add(new DoubleDataPoint(lastTimeStamp + 5, 14));

		ImmutableSortedMap<String, String> remoteTags = ImmutableSortedMap.of("host", "remote");
		List<DataPoint> remoteDataPoints = new ArrayList<DataPoint>();
		remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 1, 10));
		remoteDataPoints.add(new DoubleDataPoint(lastTimeStamp + 2, 11));

		testDataStore.clear();
		testDataStore.putDataPoints(metricName, localHostTags, localhostDataPoints);
		testDataStore.putDataPoints(metricName, remoteTags, remoteDataPoints);

		// Look from data point [1] forward and return [2]
		DataPoint futureDataPoint = RollUpJob.getFutureDataPoint(datastore, metricName, now, localhostDataPoints.get(1));

		assertThat(futureDataPoint, equalTo(localhostDataPoints.get(2)));
	}

	@Test
	public void test_calculatStartTime_datapointTime()
	{
		Sampling sampling = new Sampling();
		DoubleDataPoint dataPoint = new DoubleDataPoint(123456L, 10);

		long time = RollUpJob.calculateStartTime(dataPoint, sampling, System.currentTimeMillis());

		assertThat(time, equalTo(123456L));
	}

	@Test
	public void test_calculatStartTime_samplingTime() throws ParseException
	{
		long now = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
		Sampling sampling = new Sampling(1, TimeUnit.HOURS);

		long time = RollUpJob.calculateStartTime(null, sampling, now);

		assertThat(time, equalTo(dateFormat.parse("2013-Jan-18 3:59:12.22").getTime()));
	}

	@Test(expected = NullPointerException.class)
	public void test_calculatStartTime_samplingNull_invalid()
	{
		DoubleDataPoint dataPoint = new DoubleDataPoint(123456L, 10);

		RollUpJob.calculateStartTime(dataPoint, null, System.currentTimeMillis());
	}

	@Test
	public void test_calculatEndTime_datapoint_null()
	{
		long now = System.currentTimeMillis();
		Duration executionInterval = new Duration();

		long time = RollUpJob.calculateEndTime(null, executionInterval, now);

		assertThat(time, equalTo(now));
	}

	@Test
	public void test_calculatEndTime_datapointNotNull_recentTime() throws ParseException
	{
		long now = System.currentTimeMillis();
		Duration executionInterval = new Duration();
		DoubleDataPoint dataPoint = new DoubleDataPoint(now - 2000, 10);

		long time = RollUpJob.calculateEndTime(dataPoint, executionInterval, now);

		assertThat(time, equalTo(dataPoint.getTimestamp()));
	}

	@Test
	public void test_calculatEndTime_datapointNotNull_tooOld() throws ParseException
	{
		long datapointTime = dateFormat.parse("2013-Jan-18 4:59:12.22").getTime();
		long now = System.currentTimeMillis();
		Duration executionInterval = new Duration(1, TimeUnit.DAYS);
		DoubleDataPoint dataPoint = new DoubleDataPoint(datapointTime, 10);

		long time = RollUpJob.calculateEndTime(dataPoint, executionInterval, now);

		assertThat(time, equalTo(dateFormat.parse("2013-Jan-22 4:59:12.22").getTime()));
	}

	public static class TestDatastore implements Datastore
	{
		List<ListDataPointGroup> dataPointGroups = new ArrayList<ListDataPointGroup>();

		public void clear()
		{
			dataPointGroups = new ArrayList<ListDataPointGroup>();
		}

		@Override
		public void close() throws InterruptedException, DatastoreException
		{
		}

		public void putDataPoints(String metricName, ImmutableSortedMap<String, String> tags, List<DataPoint> dataPoints) throws DatastoreException
		{
			ListDataPointGroup dataPointGroup = new ListDataPointGroup(metricName);

			for (Map.Entry<String, String> tag : tags.entrySet())
			{
				dataPointGroup.addTag(tag.getKey(), tag.getValue());
			}

			for (DataPoint dataPoint : dataPoints)
			{
				dataPointGroup.addDataPoint(dataPoint);
			}

			dataPointGroups.add(dataPointGroup);
		}

		@Subscribe
		public void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint, int ttl) throws DatastoreException
		{
			ListDataPointGroup dataPointGroup = new ListDataPointGroup(metricName);
			dataPointGroup.addDataPoint(dataPoint);

			for (Map.Entry<String, String> tag : tags.entrySet())
			{
				dataPointGroup.addTag(tag.getKey(), tag.getValue());
			}

			dataPointGroups.add(dataPointGroup);
		}

		@Override
		public Iterable<String> getMetricNames() throws DatastoreException
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<String> getTagNames() throws DatastoreException
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<String> getTagValues() throws DatastoreException
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
		{
			for (ListDataPointGroup dataPointGroup : dataPointGroups)
			{
				try
				{
					dataPointGroup.sort(query.getOrder());

					Map<String, String> tags = new HashMap<String, String>();
					for (String tagName : dataPointGroup.getTagNames())
					{
						tags.put(tagName, dataPointGroup.getTagValues(tagName).iterator().next());
					}

					DataPoint dataPoint = getNext(dataPointGroup, query);
					if (dataPoint != null)
					{
						queryCallback.startDataPointSet(dataPoint.getDataStoreDataType(), tags);
						queryCallback.addDataPoint(dataPoint);

						while (dataPointGroup.hasNext())
						{
							DataPoint next = getNext(dataPointGroup, query);
							if (next != null)
							{
								queryCallback.addDataPoint(next);
							}
						}
						queryCallback.endDataPoints();
					}
				}
				catch (IOException e)
				{
					throw new DatastoreException(e);
				}
			}
		}

		private DataPoint getNext(DataPointGroup group, DatastoreMetricQuery query)
		{
			DataPoint dataPoint = null;
			while (group.hasNext())
			{
				DataPoint dp = group.next();
				if (dp.getTimestamp() >= query.getStartTime())
				{
					dataPoint = dp;
					break;
				}
			}

			return dataPoint;
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws
				DatastoreException
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public TagSet queryMetricTags(DatastoreMetricQuery query) throws
				DatastoreException
		{
			throw new UnsupportedOperationException();
		}
	}
}