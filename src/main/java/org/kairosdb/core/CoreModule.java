/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core;

import com.google.common.net.InetAddresses;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.kairosdb.core.aggregator.*;
import org.kairosdb.core.datapoints.*;
import org.kairosdb.core.datastore.GuiceQueryPluginFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryPluginFactory;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.groupby.*;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.jobs.CacheFileCleaner;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.kairosdb.util.MemoryMonitor;
import org.kairosdb.util.Util;

import java.util.List;
import java.util.MissingResourceException;
import java.util.Properties;

public class CoreModule extends AbstractModule
{
	public static final String DATAPOINTS_FACTORY_LONG = "kairosdb.datapoints.factory.long";
	public static final String DATAPOINTS_FACTORY_DOUBLE = "kairosdb.datapoints.factory.double";
	private Properties m_props;

	public CoreModule(Properties props)
	{
		m_props = props;
	}

	private Class getClassForProperty(String property)
	{
		String className = m_props.getProperty(property);

		Class klass = null;
		try
		{
			klass = getClass().getClassLoader().loadClass(className);
		}
		catch (ClassNotFoundException e)
		{
			throw new MissingResourceException("Unable to load class", className, property);
		}

		return (klass);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void configure()
	{
		bind(QueryQueuingManager.class).in(Singleton.class);
		bind(KairosDatastore.class).in(Singleton.class);
		bind(AggregatorFactory.class).to(GuiceAggregatorFactory.class).in(Singleton.class);
		bind(GroupByFactory.class).to(GuiceGroupByFactory.class).in(Singleton.class);
		bind(QueryPluginFactory.class).to(GuiceQueryPluginFactory.class).in(Singleton.class);
		bind(QueryParser.class).in(Singleton.class);
		bind(CacheFileCleaner.class).in(Singleton.class);
		bind(KairosDBScheduler.class).in(Singleton.class);
		bind(MemoryMonitor.class).in(Singleton.class);

		bind(SumAggregator.class);
		bind(MinAggregator.class);
		bind(MaxAggregator.class);
		bind(AvgAggregator.class);
		bind(StdAggregator.class);
		bind(RateAggregator.class);
		bind(SamplerAggregator.class);
		bind(LeastSquaresAggregator.class);
		bind(PercentileAggregator.class);
		bind(DivideAggregator.class);
		bind(ScaleAggregator.class);
		bind(CountAggregator.class);
		bind(DiffAggregator.class);
		bind(DataGapsMarkingAggregator.class);
		bind(FirstAggregator.class);
		bind(LastAggregator.class);
		bind(SaveAsAggregator.class);
		bind(TrimAggregator.class);
		bind(SmaAggregator.class);


		bind(ValueGroupBy.class);
		bind(TimeGroupBy.class);
		bind(TagGroupBy.class);
		bind(BinGroupBy.class);

		Names.bindProperties(binder(), m_props);
		bind(Properties.class).toInstance(m_props);

		String hostname = m_props.getProperty("kairosdb.hostname");
		bindConstant().annotatedWith(Names.named("HOSTNAME")).to(hostname != null ? hostname: Util.getHostName());

		bind(new TypeLiteral<List<DataPointListener>>()
		{
		}).toProvider(DataPointListenerProvider.class);

		//bind datapoint default impls
		bind(DoubleDataPointFactory.class)
				.to(getClassForProperty(DATAPOINTS_FACTORY_DOUBLE)).in(Singleton.class);
		//This is required in case someone overwrites our factory property
		bind(DoubleDataPointFactoryImpl.class).in(Singleton.class);

		bind(LongDataPointFactory.class)
				.to(getClassForProperty(DATAPOINTS_FACTORY_LONG)).in(Singleton.class);
		//This is required in case someone overwrites our factory property
		bind(LongDataPointFactoryImpl.class).in(Singleton.class);

		bind(LegacyDataPointFactory.class).in(Singleton.class);

		bind(StringDataPointFactory.class).in(Singleton.class);
                
		bind(StringDataPointFactory.class).in(Singleton.class);

		bind(NullDataPointFactory.class).in(Singleton.class);

		bind(KairosDataPointFactory.class).to(GuiceKairosDataPointFactory.class).in(Singleton.class);

		String hostIp = m_props.getProperty("kairosdb.host_ip");
		bindConstant().annotatedWith(Names.named("HOST_IP")).to(hostIp != null ? hostIp: InetAddresses.toAddrString(Util.findPublicIp()));
	}
}
