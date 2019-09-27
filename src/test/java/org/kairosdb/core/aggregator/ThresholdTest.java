package org.kairosdb.core.aggregator;

import org.junit.Test;
import org.kairosdb.core.aggregator.Threshold.Boundary;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ThresholdTest
{
    @Test
    public void test_defaults_boundaryType()
    {
        Threshold threshold = new Threshold();

        assertEquals(Boundary.SUPERIOR, threshold.getBoundary());
    }

    @Test
    public void test_compareValue_superiorBoundary()
    {
        Threshold threshold = new Threshold(0, Boundary.SUPERIOR);

        assertThat(threshold.compareValue(-1), lessThan(0));
        assertThat(threshold.compareValue(0), lessThan(0));
        assertThat(threshold.compareValue(1), greaterThan(0));
    }

    @Test
    public void test_compareValue_inferiorBoundary()
    {
        Threshold threshold = new Threshold(0, Boundary.INFERIOR);

        assertThat(threshold.compareValue(-1), lessThan(0));
        assertThat(threshold.compareValue(0), greaterThan(0));
        assertThat(threshold.compareValue(1), greaterThan(0));
    }

    @Test
    public void test_compareTo()
    {
        assertThat(new Threshold(-1, Boundary.SUPERIOR).compareTo(new Threshold(0, Boundary.SUPERIOR)), lessThan(0));
        assertThat(new Threshold(0, Boundary.SUPERIOR).compareTo(new Threshold(0, Boundary.SUPERIOR)), equalTo(0));
        assertThat(new Threshold(1, Boundary.SUPERIOR).compareTo(new Threshold(0, Boundary.SUPERIOR)), greaterThan(0));

        assertThat(new Threshold(0, Boundary.INFERIOR).compareTo(new Threshold(0, Boundary.SUPERIOR)), lessThan(0));
        assertThat(new Threshold(0, Boundary.SUPERIOR).compareTo(new Threshold(0, Boundary.INFERIOR)), greaterThan(0));
    }
}
