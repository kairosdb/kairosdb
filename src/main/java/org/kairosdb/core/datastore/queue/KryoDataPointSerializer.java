package org.kairosdb.core.datastore.queue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;

import java.io.*;

/**
 Created by bhawkins on 1/7/16.
 */
public class KryoDataPointSerializer extends Serializer<DataPoint>
{
	private KairosDataPointFactory m_dataPointFactory;

	public KryoDataPointSerializer(KairosDataPointFactory kairosDataPointFactory)
	{
		m_dataPointFactory = kairosDataPointFactory;
	}


	@Override
	public void write(Kryo kryo, Output output, DataPoint dataPoint)
	{
		DataOutput dout = new DataOutputStream(output);
		try
		{
			dout.writeUTF(dataPoint.getDataStoreDataType());
			dout.writeLong(dataPoint.getTimestamp());
			dataPoint.writeValueToBuffer(dout);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public DataPoint read(Kryo kryo, Input input, Class<DataPoint> type)
	{

		DataInput din = new DataInputStream(input);
		DataPoint dataPoint = null;
		try
		{
			String storeType = din.readUTF();
			long timestamp = din.readLong();
			dataPoint = m_dataPointFactory.createDataPoint(storeType, timestamp, din);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		return dataPoint;
	}
}
