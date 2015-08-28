package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 Created by bhawkins on 8/28/15.
 */
public class TrimAggregatorTest
{
	@Test
	public void test_oneDataPointWithTrimFirst()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.FIRST);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_twoDataPointsWithTrimFirst()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.FIRST);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();

		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getLongValue(), equalTo(20L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_threeDataPointWithTrimFirst()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 30));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.FIRST);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));

		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getLongValue(), equalTo(20L));

		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getLongValue(), equalTo(30L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_oneDataPointWithTrimLast()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.LAST);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_twoDataPointsWithTrimLast()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.LAST);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dp = results.next();

		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getLongValue(), equalTo(10L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_threeDataPointWithTrimLast()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 30));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.LAST);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));

		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getLongValue(), equalTo(10L));

		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getLongValue(), equalTo(20L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_oneDataPointWithTrimBoth()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.BOTH);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_twoDataPointsWithTrimBoth()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.BOTH);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_threeDataPointWithTrimBoth()
	{
		ListDataPointGroup group = new ListDataPointGroup("trim_test");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addDataPoint(new LongDataPoint(3, 30));

		TrimAggregator trimAggregator = new TrimAggregator();
		trimAggregator.setTrim(TrimAggregator.Trim.BOTH);
		DataPointGroup results = trimAggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));

		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getLongValue(), equalTo(20L));

		assertThat(results.hasNext(), equalTo(false));
	}
}
