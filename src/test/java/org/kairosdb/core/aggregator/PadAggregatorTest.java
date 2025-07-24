package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.NullDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.kairosdb.core.DataPointMatcher.dataPoint;

public class PadAggregatorTest
{
	private PadAggregator aggregator;

	@Before
	public void setup()
	{
		aggregator = new PadAggregator();
		aggregator.setPadValue(0);
	}


	@Test
	public void test_withAPadShouldProvideOneZeroDP()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(0, 1));
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(1, 20));
		group.addDataPoint(new LongDataPoint(1, 3));
		group.addDataPoint(new LongDataPoint(2, 1));
		group.addDataPoint(new LongDataPoint(2, 3));
		group.addDataPoint(new LongDataPoint(4, 5));
		group.addDataPoint(new LongDataPoint(5, 25));

		aggregator.setStartTime(0);
		aggregator.setEndTime(5);
		aggregator.init();
		DataPointGroup results = aggregator.aggregate(group);

		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(0L));
		assertThat(dataPoint.getLongValue(), equalTo(1L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(3L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(1L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(3L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //padded entry

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(4L));
		assertThat(dataPoint.getLongValue(), equalTo(5L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(5L));
		assertThat(dataPoint.getLongValue(), equalTo(25L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_withSeveralPadssShouldProvideZeroDPs()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(4, 5));
		group.addDataPoint(new LongDataPoint(5, 25));

		aggregator.setStartTime(1);
		aggregator.setEndTime(5);
		aggregator.init();
		DataPointGroup results = aggregator.aggregate(group);

		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //Pad


		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //pad


		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(4L));
		assertThat(dataPoint.getLongValue(), equalTo(5L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(5L));
		assertThat(dataPoint.getLongValue(), equalTo(25L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_withPadsBeforeAndAfterData()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(3, 10));
		group.addDataPoint(new LongDataPoint(4, 5));
		group.addDataPoint(new LongDataPoint(5, 25));

		aggregator.setStartTime(1);
		aggregator.setEndTime(7);
		aggregator.init();
		DataPointGroup results = aggregator.aggregate(group);

		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //pad

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //pad


		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(4L));
		assertThat(dataPoint.getLongValue(), equalTo(5L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(5L));
		assertThat(dataPoint.getLongValue(), equalTo(25L));


		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(6L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //pad

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(7L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //pad

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_withTrimPadssBeforeAndAfterData()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(3, 10));

		group.addDataPoint(new LongDataPoint(5, 25));

		aggregator.setStartTime(1);
		aggregator.setEndTime(7);
		aggregator.setTrim(true);
		aggregator.init();
		DataPointGroup results = aggregator.aggregate(group);


		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(4L));
		assertThat(dataPoint.getLongValue(), equalTo(0L)); //pad

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(5L));
		assertThat(dataPoint.getLongValue(), equalTo(25L));


		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_shouldBeEndInclusiveIfEndIsNotAligned()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(0, 0));
		group.addDataPoint(new LongDataPoint(1000, 1));
		group.addDataPoint(new LongDataPoint(2000, 2));

		aggregator.setStartTime(0);
		aggregator.setAlignStartTime(true);
		aggregator.setSampling(new Sampling(1, TimeUnit.SECONDS));
		aggregator.setEndTime(3001);
		aggregator.init();

		assertThat(
				ImmutableList.copyOf(aggregator.aggregate(group)),
				Matchers.contains(
						ImmutableList.of(
								dataPoint(0, 0L),
								dataPoint(1000, 1L),
								dataPoint(2000, 2L),
								dataPoint(3000, 0L)))); //Pad
	}

	@Test
	public void test_shouldBeEndInclusiveIfNoDataIsIncludedAtEndTimestamp()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(0, 0));
		group.addDataPoint(new LongDataPoint(1000, 1));
		group.addDataPoint(new LongDataPoint(2000, 2));

		aggregator.setStartTime(0);
		aggregator.setAlignStartTime(true);
		aggregator.setSampling(new Sampling(1, TimeUnit.SECONDS));
		aggregator.setEndTime(3001);
		aggregator.init();

		assertThat(
				ImmutableList.copyOf(aggregator.aggregate(group)),
				Matchers.contains(
						ImmutableList.of(
								dataPoint(0, 0L),
								dataPoint(1000, 1L),
								dataPoint(2000, 2L),
								dataPoint(3000, 0L)))); //Pad
	}

	@Test
	public void test_shouldBeEndInclusiveIfDataIsIncludedAtEndTimestamp()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(0, 0));
		group.addDataPoint(new LongDataPoint(1000, 1));
		group.addDataPoint(new LongDataPoint(2000, 2));
		group.addDataPoint(new LongDataPoint(3000, 3));

		aggregator.setStartTime(0);
		aggregator.setAlignStartTime(true);
		aggregator.setSampling(new Sampling(1, TimeUnit.SECONDS));
		aggregator.setEndTime(3000);
		aggregator.init();

		assertThat(
				ImmutableList.copyOf(aggregator.aggregate(group)),
				Matchers.contains(
						ImmutableList.of(
								dataPoint(0, 0L),
								dataPoint(1000, 1L),
								dataPoint(2000, 2L),
								dataPoint(3000, 3L))));
	}
}
