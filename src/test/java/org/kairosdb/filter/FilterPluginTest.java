package org.kairosdb.filter;

import com.google.common.collect.ImmutableSortedMap;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.events.DataPointEvent;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;


public class FilterPluginTest
{
	private final LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

	@Before
	public void setUp() throws Exception
	{
	}

	private DataPointEvent createEvent(String metricName)
	{
		return new DataPointEvent(metricName, ImmutableSortedMap.of(),
				m_dataPointFactory.createDataPoint(0L, 0L));
	}

	private void assertNoFilter(FilterPlugin filter, String metricName)
	{
		DataPointEvent event = createEvent(metricName);
		assertThat(filter.filterDataPoint(event)).isEqualTo(event);
	}

	private void assertFilter(FilterPlugin filter, String metricName)
	{
		DataPointEvent event = createEvent(metricName);
		assertThat(filter.filterDataPoint(event)).isNull();
	}

	@Test
	public void test_setList() throws KairosDBException
	{
		FilterPlugin filter = new FilterPlugin();
		filter.setList(Arrays.asList("metric.one", "metric.two"));
		filter.start();

		assertNoFilter(filter, "metric.three");
		assertFilter(filter, "metric.one");
		assertFilter(filter, "metric.two");
		assertNoFilter(filter, "metric");
		assertNoFilter(filter, "metric.one.two");
	}

	@Test
	public void test_setPrefixList() throws KairosDBException
	{
		FilterPlugin filter = new FilterPlugin();
		filter.setPrefixList(Arrays.asList("metric.one", "metric.two"));
		filter.start();

		assertNoFilter(filter, "metric.three");
		assertFilter(filter, "metric.one");
		assertFilter(filter, "metric.two");
		assertNoFilter(filter, "metric");
		assertFilter(filter, "metric.one.two");
		assertFilter(filter, "metric.two.and.then.some");
	}

	@Test
	public void test_setRegexList() throws KairosDBException
	{
		FilterPlugin filter = new FilterPlugin();
		filter.setRegexList(Arrays.asList(".*metric.*", ".*bad"));
		filter.start();

		assertFilter(filter, "metric.one");
		assertFilter(filter, "metric.two");
		assertFilter(filter, "metric");
		assertFilter(filter, "start.metric.one.two");
		assertFilter(filter, "something.bad");
		assertNoFilter(filter, "something.badNot");
		assertNoFilter(filter, "good.value");
	}

	@Test
	public void test_combination() throws KairosDBException
	{
		FilterPlugin filter = new FilterPlugin();
		filter.setList(Arrays.asList("bad.guy"));
		filter.setPrefixList(Arrays.asList("nope."));
		filter.setRegexList(Arrays.asList(".*metric.*"));
		filter.start();

		assertNoFilter(filter,"good.metri");
		assertFilter(filter, "bad.guy");
		assertFilter(filter, "nope.cannot_like_this_one");
		assertFilter(filter, "notgood.metric");
	}

	@Test
	public void test_noFilter() throws KairosDBException
	{
		FilterPlugin filter = new FilterPlugin();
		filter.start();

		assertNoFilter(filter,"good.metri");
		assertNoFilter(filter, "bad.guy");
		assertNoFilter(filter, "nope.cannot_like_this_one");
		assertNoFilter(filter, "notgood.metric");
	}
}
