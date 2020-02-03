package org.kairosdb.core.aggregator;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.ScoreAggregator.Order;
import org.kairosdb.core.aggregator.Threshold.Boundary;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.EmptyDataPointGroup;
import org.kairosdb.core.datastore.TagSetImpl;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class ScoreAggregatorTest
{
    private ScoreAggregator aggregator;

    @Before
    public void setup()
    {
        aggregator = new ScoreAggregator(new DoubleDataPointFactoryImpl());
    }

    @Test
    public void test_defaultDirection()
    {
        assertEquals(Order.ASCENDING, aggregator.getOrder());
    }

    @Test(expected = NullPointerException.class)
    public void test_aggregate_nullSet()
    {
        aggregator.aggregate(null);
    }

    @Test
    public void test_aggregate_emptySet()
    {
        DataPointGroup results = aggregator.aggregate(new EmptyDataPointGroup("Test.Metric", new TagSetImpl()));

        assertFalse(results.hasNext());
    }

    @Test
    public void test_setThresholds_thresholdsShouldBeSorted()
    {
        aggregator.setThresholds(new Threshold[]{
                new Threshold(3, Boundary.INFERIOR),
                new Threshold(1, Boundary.INFERIOR),
                new Threshold(2, Boundary.INFERIOR)
        });

        assertThat(aggregator.getThresholds(), equalTo(new Threshold[]{
                new Threshold(1, Boundary.INFERIOR),
                new Threshold(2, Boundary.INFERIOR),
                new Threshold(3, Boundary.INFERIOR)
        }));
    }

    @Test
    public void test_aggregate_ascendingThresholds()
    {
        aggregator.setThresholds(new Threshold[]{
                new Threshold(0, Boundary.INFERIOR),
                new Threshold(10, Boundary.INFERIOR)
        });
        aggregator.setOrder(Order.ASCENDING);

        ListDataPointGroup group = new ListDataPointGroup("foo");
        group.addDataPoint(new DoubleDataPoint(0, -1));
        group.addDataPoint(new DoubleDataPoint(1, 1));
        group.addDataPoint(new DoubleDataPoint(2, 11));

        DataPointGroup results = aggregator.aggregate(group);

        DataPoint dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(0L));
        assertThat(dataPoint.getDoubleValue(), equalTo(0.0));

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(1L));
        assertThat(dataPoint.getDoubleValue(), equalTo(1.0));

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(2L));
        assertThat(dataPoint.getDoubleValue(), equalTo(2.0));
    }


    @Test
    public void test_aggregate_descendingThresholds()
    {
        aggregator.setThresholds(new Threshold[]{
                new Threshold(0, Boundary.INFERIOR),
                new Threshold(10, Boundary.INFERIOR)
        });
        aggregator.setOrder(Order.DESCENDING);

        ListDataPointGroup group = new ListDataPointGroup("foo");
        group.addDataPoint(new DoubleDataPoint(0, -1));
        group.addDataPoint(new DoubleDataPoint(1, 1));
        group.addDataPoint(new DoubleDataPoint(2, 11));

        DataPointGroup results = aggregator.aggregate(group);

        DataPoint dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(0L));
        assertThat(dataPoint.getDoubleValue(), equalTo(2.0));

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(1L));
        assertThat(dataPoint.getDoubleValue(), equalTo(1.0));

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(2L));
        assertThat(dataPoint.getDoubleValue(), equalTo(0.0));
    }

    @Test
    public void test_aggregate_inferiorBoundary()
    {
        aggregator.setThresholds(new Threshold[]{
                new Threshold(0, Boundary.INFERIOR)
        });

        //ascending thresholds
        aggregator.setOrder(Order.ASCENDING);

        ListDataPointGroup group = new ListDataPointGroup("foo");
        group.addDataPoint(new DoubleDataPoint(0, 0));

        DataPointGroup results = aggregator.aggregate(group);

        DataPoint dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(0L));
        assertThat(dataPoint.getDoubleValue(), equalTo(1.0));

        //descending thresholds
        aggregator.setOrder(Order.DESCENDING);

        group = new ListDataPointGroup("foo");
        group.addDataPoint(new DoubleDataPoint(0, 0));

        results = aggregator.aggregate(group);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(0L));
        assertThat(dataPoint.getDoubleValue(), equalTo(0.0));
    }

    @Test
    public void test_aggregate_superiorBoundary()
    {
        aggregator.setThresholds(new Threshold[]{
                new Threshold(0, Boundary.SUPERIOR)
        });

        //ascending thresholds
        aggregator.setOrder(Order.ASCENDING);

        ListDataPointGroup group = new ListDataPointGroup("foo");
        group.addDataPoint(new DoubleDataPoint(0, 0));

        DataPointGroup results = aggregator.aggregate(group);

        DataPoint dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(0L));
        assertThat(dataPoint.getDoubleValue(), equalTo(0.0));

        //descending thresholds
        aggregator.setOrder(Order.DESCENDING);

        group = new ListDataPointGroup("foo");
        group.addDataPoint(new DoubleDataPoint(0, 0));

        results = aggregator.aggregate(group);

        dataPoint = results.next();
        assertThat(dataPoint.getTimestamp(), equalTo(0L));
        assertThat(dataPoint.getDoubleValue(), equalTo(1.0));
    }
}
