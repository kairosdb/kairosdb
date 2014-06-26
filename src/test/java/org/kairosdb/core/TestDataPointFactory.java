package org.kairosdb.core;

import com.google.gson.JsonElement;
import org.kairosdb.core.datapoints.*;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/9/13
 Time: 8:33 AM
 To change this template use File | Settings | File Templates.
 */
public class TestDataPointFactory implements KairosDataPointFactory
{
	private Map<String, DataPointFactory> m_factoryMapDataStore = new HashMap<String, DataPointFactory>();
	private Map<String, DataPointFactory> m_factoryMapRegistered = new HashMap<String, DataPointFactory>();

	private void addFactory(String type, DataPointFactory factory)
	{
		m_factoryMapRegistered.put(type, factory);
		m_factoryMapDataStore.put(factory.getDataStoreType(), factory);
	}

	public TestDataPointFactory()
	{
		addFactory("long", new LongDataPointFactoryImpl());
		addFactory("double", new DoubleDataPointFactoryImpl());
		addFactory("legacy", new LegacyDataPointFactory());
		addFactory("string", new StringDataPointFactory());
	}

	@Override
	public DataPoint createDataPoint(String type, long timestamp, JsonElement json) throws IOException
	{
		DataPointFactory factory = m_factoryMapRegistered.get(type);

		DataPoint dp = factory.getDataPoint(timestamp, json);

		return (dp);
	}

	@Override
	public DataPoint createDataPoint(String type, long timestamp, DataInput buffer) throws IOException
	{
		DataPointFactory factory = m_factoryMapDataStore.get(type);

		DataPoint dp = factory.getDataPoint(timestamp, buffer);

		return (dp);
	}

	@Override
	public DataPointFactory getFactoryForType(String type)
	{
		return m_factoryMapRegistered.get(type);
	}

	@Override
	public DataPointFactory getFactoryForDataStoreType(String dataStoreType)
	{
		return m_factoryMapDataStore.get(dataStoreType);
	}

	@Override
	public String getGroupType(String datastoreType)
	{
		return getFactoryForDataStoreType(datastoreType).getGroupType();
	}

	@Override
	public boolean isRegisteredType(String type)
	{
		return m_factoryMapRegistered.containsKey(type);
	}
}
