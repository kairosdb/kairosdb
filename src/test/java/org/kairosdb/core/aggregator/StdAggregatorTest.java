// KairosDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
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