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

package org.kairosdb.core.aggregator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Arrays;
import java.util.Random;

import static java.lang.Math.floor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

public class PercentileAggregatorTest
{
	private PercentileAggregator aggregator;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void setUp()
	{
		aggregator = new PercentileAggregator(new DoubleDataPointFactoryImpl());
	}

	@Test(expected = NullPointerException.class)
	public void test_nullSet_invalid()
	{
		aggregator.aggregate(null);
	}

	private double getActualPercentile(double percentile, double[] values)
	{
		Arrays.sort(values);
		final double pos = percentile * (values.length + 1);

		if (pos < 1) {
			return values[0];
		}

		if (pos >= values.length) {
			return values[values.length - 1];
		}

		final double lower = values[(int) pos - 1];
		final double upper = values[(int) pos];
		return lower + (pos - floor(pos)) * (upper - lower);
	}


	private double getActualPercentile(double percentile, long[] values)
	{
		double[] doubleValues = new double[values.length];
		for (int i = 0; i < values.length; i++)
		{
			doubleValues[i] = values[i];
		}
		return getActualPercentile(percentile, doubleValues);
	}

	private double getActualPercentile(double percentile, Object[] values)
	{
		double[] doubleValues = new double[values.length];
		for (int i = 0; i < values.length; i++)
		{
			doubleValues[i] = Double.valueOf(values[i].toString());
		}
		return getActualPercentile(percentile, doubleValues);
	}

	private void test_percentileValue_double(double percentile, int testSize)
	{
		Random random = new Random();
		aggregator.setPercentile(percentile);
		ListDataPointGroup group = new ListDataPointGroup("group");
		double[] values = new double[testSize];
		for (int i = 0; i < testSize; i++)
		{
			double j = random.nextDouble();
			group.addDataPoint(new DoubleDataPoint(1, j));
			values[i] = j;
		}
		DataPointGroup results = aggregator.aggregate(group);
		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		double expected = getActualPercentile(percentile, values);
		double epsilon = 0.10 * expected;
		assertThat(dataPoint.getDoubleValue(), closeTo(expected, epsilon));

		assertThat(results.hasNext(), equalTo(false));
	}

	private void test_percentileValue_long(double percentile, int testSize)
	{
		Random random = new Random();
		aggregator.setPercentile(percentile);
		ListDataPointGroup group = new ListDataPointGroup("group");
		long[] values = new long[testSize];
		long range = 1000000000L;
		for (int i = 0; i < testSize; i++)
		{
			long j = (long)(random.nextDouble()*range);
			group.addDataPoint(new LongDataPoint(1, j));
			values[i] = j;
		}
		DataPointGroup results = aggregator.aggregate(group);
		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		double expected = getActualPercentile(percentile, values);
		double epsilon = (0.10 * expected);
		assertThat((double) dataPoint.getLongValue(), closeTo(expected, epsilon));

		assertThat(results.hasNext(), equalTo(false));
	}

	private void test_percentileValue_mixedTypeValues(double percentile, int testSize)
	{
		Random random = new Random();
		aggregator.setPercentile(percentile);
		ListDataPointGroup group = new ListDataPointGroup("group");
		Object[] values = new Object[testSize];
		long range = 1000000000L;
		for (int i = 0; i < testSize; i++)
		{
			if(random.nextBoolean())
			{
				long j = (long)(random.nextDouble()*range);
				group.addDataPoint(new LongDataPoint(1, j));
				values[i] = j;
			}
			else
			{
				double j = (random.nextDouble()*range);
				group.addDataPoint(new DoubleDataPoint(1, j));
				values[i] = j;
			}
		}
		DataPointGroup results = aggregator.aggregate(group);
		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		double expected = getActualPercentile(percentile, values);
		double epsilon = (0.10 * expected);
		assertThat((double) dataPoint.getLongValue(), closeTo(expected, epsilon));

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_longValues()
	{
		test_percentileValue_long(0.75, 10);
		test_percentileValue_long(0.90, 10);
		test_percentileValue_long(0.95, 10);
		test_percentileValue_long(0.98, 10);
		test_percentileValue_long(0.999, 10);
		test_percentileValue_long(0.75, 100);
		test_percentileValue_long(0.90, 100);
		test_percentileValue_long(0.95, 100);
		test_percentileValue_long(0.98, 100);
		test_percentileValue_long(0.999, 100);
		test_percentileValue_long(0.75, 10000);
		test_percentileValue_long(0.90, 10000);
		test_percentileValue_long(0.95, 10000);
		test_percentileValue_long(0.98, 10000);
		test_percentileValue_long(0.999, 10000);
	}

	@Test
	public void test_doubleValues()
	{
		test_percentileValue_double(0.75, 10);
		test_percentileValue_double(0.90, 10);
		test_percentileValue_double(0.95, 10);
		test_percentileValue_double(0.98, 10);
		test_percentileValue_double(0.999, 10);
		test_percentileValue_double(0.75, 100);
		test_percentileValue_double(0.90, 100);
		test_percentileValue_double(0.95, 100);
		test_percentileValue_double(0.98, 100);
		test_percentileValue_double(0.999, 100);
		test_percentileValue_double(0.75, 10000);
		test_percentileValue_double(0.90, 10000);
		test_percentileValue_double(0.95, 10000);
		test_percentileValue_double(0.98, 10000);
		test_percentileValue_double(0.999, 10000);
	}

	@Test
	public void test_mixedTypeValues()
	{
		test_percentileValue_mixedTypeValues(0.75, 10);
		test_percentileValue_mixedTypeValues(0.90, 10);
		test_percentileValue_mixedTypeValues(0.95, 10);
		test_percentileValue_mixedTypeValues(0.98, 10);
		test_percentileValue_mixedTypeValues(0.999, 10);
		test_percentileValue_mixedTypeValues(0.75, 100);
		test_percentileValue_mixedTypeValues(0.90, 100);
		test_percentileValue_mixedTypeValues(0.95, 100);
		test_percentileValue_mixedTypeValues(0.98, 100);
		test_percentileValue_mixedTypeValues(0.999, 100);
		test_percentileValue_mixedTypeValues(0.75, 10000);
		test_percentileValue_mixedTypeValues(0.90, 10000);
		test_percentileValue_mixedTypeValues(0.95, 10000);
		test_percentileValue_mixedTypeValues(0.98, 10000);
		test_percentileValue_mixedTypeValues(0.999, 10000);
	}

	@Test
	public void test_noValues()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");

		DataPointGroup results = aggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(false));
	}

	@Test
	public void test_invalidPercentiles()
	{
		exception.expect(IllegalArgumentException.class);
		test_percentileValue_long(5, 10);

		exception.expect(IllegalArgumentException.class);
		test_percentileValue_double(1.2, 10);

		exception.expect(IllegalArgumentException.class);
		test_percentileValue_mixedTypeValues(-2, 10);

		exception.expect(IllegalArgumentException.class);
		test_percentileValue_long(1.00001, 10);
	}

}
