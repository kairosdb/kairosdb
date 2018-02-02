/*
 * Copyright 2016 KairosDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.core;

import com.google.gson.JsonElement;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/10/13
 Time: 10:25 AM
 To change this template use File | Settings | File Templates.
 */
public class GuiceKairosDataPointFactory implements KairosDataPointFactory
{
	public static final Logger logger = LoggerFactory.getLogger(GuiceKairosDataPointFactory.class);
	public static final String DATAPOINTS_FACTORY_PROP_PREFIX = "kairosdb.datapoints.factory.";

	private Map<String, DataPointFactory> m_factoryMapDataStore = new HashMap<String, DataPointFactory>();
	private Map<String, DataPointFactory> m_factoryMapRegistered = new HashMap<String, DataPointFactory>();


	@Inject
	public GuiceKairosDataPointFactory(Injector injector, Properties props)
	{
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (DataPointFactory.class.isAssignableFrom(bindingClass))
			{
				DataPointFactory factory = (DataPointFactory)injector.getInstance(bindingClass);
				String dsType = factory.getDataStoreType();
				DataPointFactory registered = m_factoryMapDataStore.put(dsType, factory);
				//Check if two different classes were bound to the same data type.
				//In some cases a class may be bound in more than one place.
				if (registered != null && registered != factory)
				{
					logger.error("Multiple classes registered for data store type.  Registered {} but {} was already registered for type {}",
							factory.getClass(), registered.getClass(), dsType);
				}
			}
		}

		for (Object prop : props.keySet())
		{
			String key = (String)prop;
			if (key.startsWith(DATAPOINTS_FACTORY_PROP_PREFIX))
			{
				String className = props.getProperty(key);
				String type = key.substring(DATAPOINTS_FACTORY_PROP_PREFIX.length());
				try
				{
					Class<?> factoryClass = Class.forName(className);

					DataPointFactory factory = (DataPointFactory) injector.getInstance(factoryClass);

					m_factoryMapRegistered.put(type, factory);
				}
				catch (ClassNotFoundException e)
				{
					logger.error("Unable to load class {} specified by property {}.", className, key);
				}
			}
		}
	}

	/**
	 Creates DataPoint using the registered type
	 @return
	  @param type registered type in the configuration file
	 @param timestamp
	 @param json
	 */
	@Override
	public DataPoint createDataPoint(String type, long timestamp, JsonElement json) throws IOException
	{
		DataPointFactory factory = m_factoryMapRegistered.get(type);

		if (factory == null)
			throw new IOException("Unable to find data point factory for type: "+type);

		DataPoint dp = factory.getDataPoint(timestamp, json);

		return (dp);
	}

	/**
	 Creates a DataPoint using the data store type.
	 @param dataStoreType Internal data store type.
	 @param timestamp
	 @param buffer
	 @return
	 */
	@Override
	public DataPoint createDataPoint(String dataStoreType, long timestamp, DataInput buffer) throws IOException
	{
		DataPointFactory factory = m_factoryMapDataStore.get(dataStoreType);

		if (factory == null)
			throw new IOException("Unable to find data point factory for store type: "+dataStoreType);

		DataPoint dp = factory.getDataPoint(timestamp, buffer);

		return (dp);
	}

	/*public DataPoint createDataPoint(byte type, long timestamp, ByteBuffer buffer)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	public byte getTypeByte(String type)
	{
		return 0;  //To change body of implemented methods use File | Settings | File Templates.
	}*/

	/**
	 Locate a DataPointFactory for the specified data point type.
	 @param type publicly registered data type
	 @return
	 */
	@Override
	public DataPointFactory getFactoryForType(String type)
	{
		return m_factoryMapRegistered.get(type);
	}

	/**
	 Locate a DataPointFactory for the specified data store type.
	 @param dataStoreType Internally registered type for a data point.
	 @return
	 */
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
