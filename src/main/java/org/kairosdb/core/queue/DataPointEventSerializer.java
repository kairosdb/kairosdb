package org.kairosdb.core.queue;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;

/**
 Created by bhawkins on 10/25/16.
 */
public class DataPointEventSerializer
{
	public static final Logger logger = LoggerFactory.getLogger(DataPointEventSerializer.class);

	private final KairosDataPointFactory m_kairosDataPointFactory;

	@Inject
	public DataPointEventSerializer(KairosDataPointFactory kairosDataPointFactory)
	{
		m_kairosDataPointFactory = kairosDataPointFactory;
	}

	public byte[] serializeEvent(DataPointEvent dataPointEvent)
	{
		//Todo: Create some adaptive value here, keep stats on if the buffer increases and slowely increase it
		ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput(64);
		dataOutput.writeUTF(dataPointEvent.getMetricName());
		dataOutput.writeInt(dataPointEvent.getTtl());
		dataOutput.writeLong(dataPointEvent.getDataPoint().getTimestamp());
		dataOutput.writeUTF(dataPointEvent.getDataPoint().getDataStoreDataType());
		try
		{
			dataPointEvent.getDataPoint().writeValueToBuffer(dataOutput);
		}
		catch (IOException e)
		{
			logger.error("Error serializing DataPoint", e);
		}

		dataOutput.writeInt(dataPointEvent.getTags().size());
		for (Map.Entry<String, String> entry : dataPointEvent.getTags().entrySet())
		{
			dataOutput.writeUTF(entry.getKey());
			dataOutput.writeUTF(entry.getValue());
		}

		return dataOutput.toByteArray();
	}

	DataPointEvent deserializeEvent(byte[] bytes)
	{
		DataPointEvent ret = null;
		try
		{
			ByteArrayDataInput dataInput = ByteStreams.newDataInput(bytes);
			String metricName = dataInput.readUTF();
			int ttl = dataInput.readInt();
			long timestamp = dataInput.readLong();
			String storeType = dataInput.readUTF();

			DataPoint dataPoint = m_kairosDataPointFactory.createDataPoint(storeType, timestamp, dataInput);

			int tagCount = dataInput.readInt();
			ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
			for (int I = 0; I < tagCount; I++)
			{
				builder.put(dataInput.readUTF(), dataInput.readUTF());
			}

			ret = new DataPointEvent(metricName, builder.build(), dataPoint, ttl);

		}
		catch (IOException e)
		{
			logger.error("Unable to deserialize event", e);
		}

		return ret;
	}
}
