/*
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
package org.kairosdb.core.aggregator;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.NullDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DataGapsMarkingAggregatorTest
{
	private DataGapsMarkingAggregator aggregator;

	@Before
	public void setup()
	{
		aggregator = new DataGapsMarkingAggregator();
	}

	@Test(expected = NullPointerException.class)
	public void test_nullSet_invalid()
	{
		aggregator.aggregate(null);
	}


	@Test(expected = IllegalArgumentException.class)
	public void test_getValueFromNullDpIsIllegal()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(4, 5));
		group.addDataPoint(new LongDataPoint(5, 25));

		aggregator.setStartTime(1);
		aggregator.setEndTime(5);
		DataPointGroup results = aggregator.aggregate(group);

		results.next();
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));


	}

	@Test
	public void test_withAGapShouldProvideOneNullDP()
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
		assertThat(dataPoint instanceof NullDataPoint, equalTo(true));
		assertThat(dataPoint.isDouble(), equalTo(false));
		assertThat(dataPoint.isLong(), equalTo(false));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(4L));
		assertThat(dataPoint.getLongValue(), equalTo(5L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(5L));
		assertThat(dataPoint.getLongValue(), equalTo(25L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_withSeveralGapsShouldProvideNullDPs()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(4, 5));
		group.addDataPoint(new LongDataPoint(5, 25));

		aggregator.setStartTime(1);
		aggregator.setEndTime(5);
		DataPointGroup results = aggregator.aggregate(group);

		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint instanceof NullDataPoint, equalTo(true));


		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint instanceof NullDataPoint, equalTo(true));


		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(4L));
		assertThat(dataPoint.getLongValue(), equalTo(5L));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(5L));
		assertThat(dataPoint.getLongValue(), equalTo(25L));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_withGapsBeforeAndAfterData()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(3, 10));
		group.addDataPoint(new LongDataPoint(4, 5));
		group.addDataPoint(new LongDataPoint(5, 25));

		aggregator.setStartTime(1);
		aggregator.setEndTime(7);
		DataPointGroup results = aggregator.aggregate(group);

		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint instanceof NullDataPoint, equalTo(true));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint instanceof NullDataPoint, equalTo(true));


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
		assertThat(dataPoint instanceof NullDataPoint, equalTo(true));

		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(7L));
		assertThat(dataPoint instanceof NullDataPoint, equalTo(true));

		assertThat(results.hasNext(), equalTo(false));
	}

}
