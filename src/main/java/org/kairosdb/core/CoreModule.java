/*
 * Copyright 2016 KairosDB Authors
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

import com.google.common.eventbus.EventBus;
import com.google.common.net.InetAddresses;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import org.apache.commons.math3.analysis.function.Sin;
import org.kairosdb.core.aggregator.*;
import org.kairosdb.core.datapoints.*;
import org.kairosdb.core.datastore.GuiceQueryPluginFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryPluginFactory;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.groupby.*;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.jobs.CacheFileCleaner;
import org.kairosdb.core.queue.DataPointEventSerializer;
import org.kairosdb.core.queue.QueueProcessor;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.kairosdb.core.scheduler.KairosDBSchedulerImpl;
import org.kairosdb.util.CongestionExecutorService;
import org.kairosdb.util.MemoryMonitor;
import org.kairosdb.util.Util;
import se.ugli.bigqueue.BigArray;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.List;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.kairosdb.core.queue.QueueProcessor.QUEUE_PROCESSOR;
import static org.kairosdb.core.queue.QueueProcessor.QUEUE_PROCESSOR_CLASS;

public class CoreModule extends AbstractModule
{
	public static final String QUEUE_PATH = "kairosdb.queue_processor.queue_path";
	public static final String PAGE_SIZE = "kairosdb.queue_processor.page_size";

	public static final String DATAPOINTS_FACTORY_LONG = "kairosdb.datapoints.factory.long";
	public static final String DATAPOINTS_FACTORY_DOUBLE = "kairosdb.datapoints.factory.double";
	private Properties m_props;
	private final EventBus m_eventBus = new EventBus();

	public CoreModule(Properties props)
	{
		m_props = props;
	}

	@SuppressWarnings("rawtypes")
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
		/*
		This bit of magic makes it so any object that is bound through guice just
		needs to annotate a method with @Subscribe and they can get events.
		 */
		bind(EventBus.class).toInstance(m_eventBus);
		//Need to register an exception handler
		bindListener(Matchers.any(), new TypeListener()
		{
			public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter)
			{
				typeEncounter.register(new InjectionListener<I>()
				{
					public void afterInjection(I i)
					{
						m_eventBus.register(i);
					}
				});
			}
		});

		bind(QueryQueuingManager.class).in(Singleton.class);
		bind(KairosDatastore.class).in(Singleton.class);
		bind(AggregatorFactory.class).to(GuiceAggregatorFactory.class).in(Singleton.class);
		bind(GroupByFactory.class).to(GuiceGroupByFactory.class).in(Singleton.class);
		bind(QueryPluginFactory.class).to(GuiceQueryPluginFactory.class).in(Singleton.class);
		bind(QueryParser.class).in(Singleton.class);
		bind(CacheFileCleaner.class).in(Singleton.class);
		bind(KairosDBScheduler.class).to(KairosDBSchedulerImpl.class).in(Singleton.class);
		bind(KairosDBSchedulerImpl.class).in(Singleton.class);
		bind(MemoryMonitor.class).in(Singleton.class);
		bind(DataPointEventSerializer.class).in(Singleton.class);

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
		bind(FilterAggregator.class);

		bind(ValueGroupBy.class);
		bind(TimeGroupBy.class);
		bind(TagGroupBy.class);
		bind(BinGroupBy.class);

		Names.bindProperties(binder(), m_props);
		bind(Properties.class).toInstance(m_props);

		String hostname = m_props.getProperty("kairosdb.hostname");
		bindConstant().annotatedWith(Names.named("HOSTNAME")).to(hostname != null ? hostname: Util.getHostName());

		//bind queue processor impl
		//Have to bind the class directly so metrics reporter can get metrics off of them
		bind(getClassForProperty(QUEUE_PROCESSOR_CLASS)).in(Singleton.class);
		bind(QueueProcessor.class)
				.to(getClassForProperty(QUEUE_PROCESSOR_CLASS)).in(Singleton.class);

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

		bind(CongestionExecutorService.class);

		String hostIp = m_props.getProperty("kairosdb.host_ip");
		bindConstant().annotatedWith(Names.named("HOST_IP")).to(hostIp != null ? hostIp: InetAddresses.toAddrString(Util.findPublicIp()));
	}

	@Provides
	@Singleton
	public BigArray getBigArray(@Named(QUEUE_PATH) String queuePath,
			@Named(PAGE_SIZE) int pageSize)
	{
		return new BigArray(queuePath, "kairos_queue", pageSize);
	}

	@Provides @Named(QUEUE_PROCESSOR) @Singleton
	public Executor getQueueExecutor()
	{
		return Executors.newSingleThreadExecutor();
	}
}
