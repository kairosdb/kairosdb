package net.opentsdb.core.aggregator;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.testing.ListDataPointGroup;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 3/5/13
 Time: 2:54 PM
 To change this template use File | Settings | File Templates.
 */
public class RateAggregatorTest
{
	@Test
	public void test_steadyRate()
	{
		ListDataPointGroup group = new ListDataPointGroup("rate");
		group.addDataPoint(new DataPoint(1, 10));
		group.addDataPoint(new DataPoint(2, 20));
		group.addDataPoint(new DataPoint(3, 30));
		group.addDataPoint(new DataPoint(4, 40));

		RateAggregator rateAggregator = new RateAggregator();
		DataPointGroup results = rateAggregator.aggregate(group);

		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(0.1));

		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getDoubleValue(), equalTo(0.1));

		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getDoubleValue(), equalTo(0.1));

		/*dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getDoubleValue(), equalTo(0.0));*/
	}


	//Test multiple data point on same time

}
