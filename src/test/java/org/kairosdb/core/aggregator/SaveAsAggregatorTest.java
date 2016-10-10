package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.groupby.TagGroupBy;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.testing.ListDataPointGroup;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.kairosdb.util.DataPointEventUtil.verifyEvent;
import static org.mockito.Mockito.mock;

/**
 Created by bhawkins on 2/9/16.
 */
public class SaveAsAggregatorTest
{
	private SaveAsAggregator m_aggregator;
	private EventBus m_mockEventBus;
	ArgumentCaptor<DataPointEvent> m_event;


	@Before
	public void setup()
	{
		m_mockEventBus = mock(EventBus.class);
		m_aggregator = new SaveAsAggregator(m_mockEventBus);

		m_event = ArgumentCaptor.forClass(DataPointEvent.class);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testTtl() throws DatastoreException
	{
		m_aggregator.setMetricName("testTtl");
		m_aggregator.setTtl(42);

		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));

		DataPointGroup results = m_aggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		verifyEvent(m_mockEventBus, "testTtl", dataPoint, 42);

		assertThat(results.hasNext(), equalTo(true));
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		verifyEvent(m_mockEventBus, "testTtl", dataPoint, 42);

		results.close();
	}


	@Test
	@SuppressWarnings("unchecked")
	public void testNoTtl() throws DatastoreException
	{
		m_aggregator.setMetricName("testTtl");

		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));

		DataPointGroup results = m_aggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		verifyEvent(m_mockEventBus, "testTtl", dataPoint, 0);

		assertThat(results.hasNext(), equalTo(true));
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		verifyEvent(m_mockEventBus, "testTtl", dataPoint, 0);

		results.close();
	}

	@Test
	public void testNotAddingSavedFrom() throws DatastoreException
	{
		m_aggregator.setMetricName("testTtl");
		m_aggregator.setTags(ImmutableSortedMap.of("sweet_tag", "value"));
		m_aggregator.setAddSavedFrom(false);

		ImmutableSortedMap<String, String> verifyMap = ImmutableSortedMap.<String, String>naturalOrder()
				.put("sweet_tag", "value")
				.build();

		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addTag("host", "tag_should_not_be_there");

		DataPointGroup results = m_aggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		verifyEvent(m_mockEventBus, "testTtl", verifyMap, dataPoint, 0);

		assertThat(results.hasNext(), equalTo(true));
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		verifyEvent(m_mockEventBus, "testTtl", verifyMap, dataPoint, 0);

		results.close();
	}

	@Test
	public void testAddedTags() throws DatastoreException
	{
		m_aggregator.setMetricName("testTtl");
		m_aggregator.setTags(ImmutableSortedMap.of("sweet_tag", "value"));

		ImmutableSortedMap<String, String> verifyMap = ImmutableSortedMap.<String, String>naturalOrder()
				.put("saved_from", "group")
				.put("sweet_tag", "value")
				.build();

		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addTag("host", "tag_should_not_be_there");

		DataPointGroup results = m_aggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		verifyEvent(m_mockEventBus, "testTtl", verifyMap, dataPoint, 0);

		assertThat(results.hasNext(), equalTo(true));
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		verifyEvent(m_mockEventBus, "testTtl", verifyMap, dataPoint, 0);

		results.close();
	}

	@Test
	public void testGroupByTagFilter() throws DatastoreException
	{
		m_aggregator.setMetricName("testTtl");
		m_aggregator.setTtl(42);

		GroupBy groupBy = new TagGroupBy("host", "host2");

		m_aggregator.setGroupBys(Collections.singletonList(groupBy));

		ImmutableSortedMap<String, String> verifyMap = ImmutableSortedMap.<String, String>naturalOrder()
				.put("saved_from", "group")
				.put("host", "bob")
				.build();

		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new LongDataPoint(1, 10));
		group.addDataPoint(new LongDataPoint(2, 20));
		group.addTag("host", "bob");
		group.addTag("some_tag", "tag_should_not_be_there");
		group.addTag("host2", "host2_tag");
		group.addTag("host2", "wont show up because there are two");

		DataPointGroup results = m_aggregator.aggregate(group);

		assertThat(results.hasNext(), equalTo(true));
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getLongValue(), equalTo(10L));

		verifyEvent(m_mockEventBus, "testTtl", verifyMap, dataPoint, 42);

		assertThat(results.hasNext(), equalTo(true));
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getLongValue(), equalTo(20L));

		verifyEvent(m_mockEventBus, "testTtl", verifyMap, dataPoint, 42);

		results.close();
	}
}
