package org.kairosdb.core.aggregator;

import org.junit.Test;
import org.junit.Before;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 6/18/13
 Time: 11:34 AM
 To change this template use File | Settings | File Templates.
 */
public class LeastSquaresAggregatorTest
{
	LeastSquaresAggregator m_aggregator;

	@Before
	public void setup()
	{
		m_aggregator = new LeastSquaresAggregator();
		m_aggregator.setStartTime(0);
		m_aggregator.setSampling(new Sampling(1, TimeUnit.HOURS));

	}

	@Test
	public void test_noDataPoint()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");

		DataPointGroup result = m_aggregator.aggregate(group);

		assertThat(result.hasNext(), equalTo(false));
	}

	@Test
	public void test_singleDataPoint()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new DataPoint(1, 10));

		DataPointGroup result = m_aggregator.aggregate(group);

		DataPoint dp = result.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(10.0));

		assertThat(result.hasNext(), equalTo(false));

	}

	@Test
	public void test_twoDataPoints()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new DataPoint(1, 10));
		group.addDataPoint(new DataPoint(2, 20));

		DataPointGroup result = m_aggregator.aggregate(group);

		DataPoint dp = result.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(10.0));

		dp = result.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getDoubleValue(), equalTo(20.0));

		assertThat(result.hasNext(), equalTo(false));

	}


}
