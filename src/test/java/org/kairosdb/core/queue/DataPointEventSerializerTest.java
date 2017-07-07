package org.kairosdb.core.queue;

import com.google.common.collect.ImmutableSortedMap;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.events.DataPointEvent;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 Created by bhawkins on 10/25/16.
 */
public class DataPointEventSerializerTest
{
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Test
	public void test_serializeDeserialize()
	{
		KairosDataPointFactory dataPointFactory = new TestDataPointFactory();
		DataPointEventSerializer serializer = new DataPointEventSerializer(dataPointFactory);

		ImmutableSortedMap<String, String> tags =
				ImmutableSortedMap.<String, String>naturalOrder()
						.put("tag1", "val1")
						.put("tag2", "val2")
						.put("tag3", "val3").build();

		DataPoint dataPoint = m_longDataPointFactory.createDataPoint(123L, 43);
		DataPointEvent original = new DataPointEvent("new_metric", tags, dataPoint, 500);

		byte[] bytes = serializer.serializeEvent(original);

		DataPointEvent processedEvent = serializer.deserializeEvent(bytes);

		assertThat(original, equalTo(processedEvent));
	}
}
