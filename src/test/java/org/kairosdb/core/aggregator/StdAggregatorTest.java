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

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;
import org.junit.Test;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

public class StdAggregatorTest
{

	@Test
	public void test()
	{
		ListDataPointGroup group = new ListDataPointGroup("group");
		for (int i = 0; i < 10000; i++)
		{
			group.addDataPoint(new DataPoint(1, i));
		}

		StdAggregator aggregator = new StdAggregator();

		DataPointGroup dataPointGroup = aggregator.aggregate(group);

		DataPoint stdev = dataPointGroup.next();
		assertThat(stdev.getDoubleValue(), closeTo(2886.462, 0.44));
	}

	@Test
	public void test_random()
	{
		long seed = System.nanoTime();
		Random random = new Random(seed);

		long[] values = new long[1000];
		ListDataPointGroup group = new ListDataPointGroup("group");
		for (int i = 0; i < values.length; i++)
		{
			long randomValue = random.nextLong();
			group.addDataPoint(new DataPoint(1, randomValue));
			values[i] = randomValue;
		}

		StdAggregator aggregator = new StdAggregator();

		DataPointGroup dataPointGroup = aggregator.aggregate(group);

		DataPoint stdev = dataPointGroup.next();
		double expected = naiveStdDev(values);
		double epsilon = 0.001 * expected;
		assertThat(stdev.getDoubleValue(), closeTo(expected, epsilon));
	}

	private static double naiveStdDev(long[] values)
	{
		double sum = 0;
		double mean = 0;

		for (final double value : values)
		{
			sum += value;
		}
		mean = sum / values.length;

		double squaresum = 0;
		for (final double value : values)
		{
			squaresum += Math.pow(value - mean, 2);
		}
		final double variance = squaresum / values.length;
		return Math.sqrt(variance);
	}
}