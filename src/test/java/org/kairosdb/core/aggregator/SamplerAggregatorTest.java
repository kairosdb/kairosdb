/*
 * Copyright 2016 KairosDB Authors
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

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SamplerAggregatorTest
{
    @Test(expected = NullPointerException.class)
    public void test_nullSet_invalid()
    {
        new SamplerAggregator(new DoubleDataPointFactoryImpl()).aggregate(null);
    }

    @Test
    public void test_steadyRate()
    {
        ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 20));
        group.addDataPoint(new LongDataPoint(3, 30));
        group.addDataPoint(new LongDataPoint(4, 40));

        SamplerAggregator samplerAggregator = new SamplerAggregator(new DoubleDataPointFactoryImpl());
        DataPointGroup results = samplerAggregator.aggregate(group);

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getDoubleValue(), equalTo(20.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(3L));
        assertThat(dp.getDoubleValue(), equalTo(30.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(4L));
        assertThat(dp.getDoubleValue(), equalTo(40.0));
    }

    @Test
    public void test_changingRate()
    {
        ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 10));
        group.addDataPoint(new LongDataPoint(3, 5));
        group.addDataPoint(new LongDataPoint(4, 20));

        SamplerAggregator samplerAggregator = new SamplerAggregator(new DoubleDataPointFactoryImpl());
        DataPointGroup results = samplerAggregator.aggregate(group);

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(3L));
        assertThat(dp.getDoubleValue(), equalTo(5.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(4L));
        assertThat(dp.getDoubleValue(), equalTo(20.0));
    }

    @Test
    public void test_changingPeriod()
    {
        ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(2, 10));
        group.addDataPoint(new LongDataPoint(4, 10));
        group.addDataPoint(new LongDataPoint(6, 20));

        SamplerAggregator samplerAggregator = new SamplerAggregator(new DoubleDataPointFactoryImpl());
        DataPointGroup results = samplerAggregator.aggregate(group);

        DataPoint dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(2L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(4L));
        assertThat(dp.getDoubleValue(), equalTo(5.0));

        dp = results.next();
        assertThat(dp.getTimestamp(), equalTo(6L));
        assertThat(dp.getDoubleValue(), equalTo(10.0));
    }


    @Test(expected = IllegalStateException.class)
    public void test_dataPointsAtSameTime()
    {
        ListDataPointGroup group = new ListDataPointGroup("rate");
        group.addDataPoint(new LongDataPoint(1, 10));
        group.addDataPoint(new LongDataPoint(1, 15));
        group.addDataPoint(new LongDataPoint(2, 5));
        group.addDataPoint(new LongDataPoint(2, 20));
        group.addDataPoint(new LongDataPoint(3, 30));

        SamplerAggregator samplerAggregator = new SamplerAggregator(new DoubleDataPointFactoryImpl());
        DataPointGroup results = samplerAggregator.aggregate(group);

        DataPoint dp = results.next();
    }

}
