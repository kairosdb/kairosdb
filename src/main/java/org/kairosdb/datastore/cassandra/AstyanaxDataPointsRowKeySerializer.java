package org.kairosdb.datastore.cassandra;

import com.netflix.astyanax.serializers.AbstractSerializer;

import java.nio.ByteBuffer;

/**
 Created by bhawkins on 12/12/16.
 */
public class AstyanaxDataPointsRowKeySerializer extends AbstractSerializer<DataPointsRowKey>
{
	private DataPointsRowKeySerializer m_serializer;

	public AstyanaxDataPointsRowKeySerializer()
	{
		m_serializer = new DataPointsRowKeySerializer();
	}

	@Override
	public ByteBuffer toByteBuffer(DataPointsRowKey obj)
	{
		return m_serializer.toByteBuffer(obj);
	}

	@Override
	public DataPointsRowKey fromByteBuffer(ByteBuffer byteBuffer)
	{
		return m_serializer.fromByteBuffer(byteBuffer);
	}
}
