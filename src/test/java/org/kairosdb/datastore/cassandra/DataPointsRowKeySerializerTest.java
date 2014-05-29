package org.kairosdb.datastore.cassandra;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DataPointsRowKeySerializerTest
{
	@Test
	public void test_toByteBuffer_oldFormat()
	{
		SortedMap<String, String> map = new TreeMap<String, String>();
		map.put("a", "b");
		map.put("c", "d");
		map.put("e", "f");

		DataPointsRowKeySerializer serializer = new DataPointsRowKeySerializer();
		ByteBuffer buffer = serializer.toByteBuffer(new DataPointsRowKey("myMetric", 12345L, "", map));

		DataPointsRowKey rowKey = serializer.fromByteBuffer(buffer);

		assertThat(rowKey.getMetricName(), equalTo("myMetric"));
		assertThat(rowKey.getDataType(), equalTo(""));
		assertThat(rowKey.getTimestamp(), equalTo(12345L));
	}

	@Test
	public void test_toByteBuffer_newFormat()
	{
		SortedMap<String, String> map = new TreeMap<String, String>();
		map.put("a", "b");
		map.put("c", "d");
		map.put("e", "f");

		DataPointsRowKeySerializer serializer = new DataPointsRowKeySerializer();
		ByteBuffer buffer = serializer.toByteBuffer(new DataPointsRowKey("myMetric", 12345L, "myDataType", map));

		DataPointsRowKey rowKey = serializer.fromByteBuffer(buffer);

		assertThat(rowKey.getMetricName(), equalTo("myMetric"));
		assertThat(rowKey.getDataType(), equalTo("myDataType"));
		assertThat(rowKey.getTimestamp(), equalTo(12345L));
		assertThat(rowKey.getTags().size(), equalTo(3));
		assertThat(rowKey.getTags().get("a"), equalTo("b"));
		assertThat(rowKey.getTags().get("c"), equalTo("d"));
		assertThat(rowKey.getTags().get("e"), equalTo("f"));

	}
}