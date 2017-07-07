package org.kairosdb.util;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.EventBus;
import org.h2.store.Data;
import org.kairosdb.core.DataPoint;
import org.kairosdb.events.DataPointEvent;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 Created by bhawkins on 10/3/16.
 */
public class DataPointEventUtil
{
	private static DataPointEvent verifyPost(EventBus eventBus)
	{
		ArgumentCaptor<DataPointEvent> event = ArgumentCaptor.forClass(DataPointEvent.class);
		verify(eventBus, timeout(5000).times(1)).post(event.capture());
		reset(eventBus);

		return event.getValue();
	}

	public static void verifyEvent(EventBus eventBus, String metricName,
			ImmutableSortedMap<String, String> tags, DataPoint dataPoint, int ttl)
	{
		DataPointEvent event = verifyPost(eventBus);
		assertThat(event.getMetricName(), equalTo(metricName));
		assertThat(event.getTags(), equalTo(tags));
		assertThat(event.getDataPoint(), equalTo(dataPoint));
		assertThat(event.getTtl(), equalTo(ttl));
	}

	public static void verifyEvent(EventBus eventBus, String metricName,
			ImmutableSortedMap<String, String> tags, DataPoint dataPoint)
	{
		DataPointEvent event = verifyPost(eventBus);
		assertThat(event.getMetricName(), equalTo(metricName));
		assertThat(event.getTags(), equalTo(tags));
		assertThat(event.getDataPoint(), equalTo(dataPoint));
	}

	public static void verifyEvent(EventBus eventBus,
			final String metricName,
			final DataPoint dataPoint,
			final int ttl)
	{
		DataPointEvent event = verifyPost(eventBus);
		assertThat(event.getMetricName(), equalTo(metricName));
		assertThat(event.getDataPoint(), equalTo(dataPoint));
		assertThat(event.getTtl(), equalTo(ttl));
	}

	public static void verifyEvent(EventBus eventBus, String metricName,
			DataPoint dataPoint)
	{
		DataPointEvent event = verifyPost(eventBus);
		assertThat(event.getMetricName(), equalTo(metricName));
		assertThat(event.getDataPoint(), equalTo(dataPoint));
	}

	private class DataPointEventMatcher extends ArgumentMatcher<DataPointEvent>
	{
		@Override
		public boolean matches(Object argument)
		{
			DataPointEvent event = (DataPointEvent)argument;
			return true;
			/*return metricName.equals(event.getMetricName()) &&
					dataPoint.equals(event.getDataPoint()) &&
					ttl == event.getTtl();*/
		}
	}
}
