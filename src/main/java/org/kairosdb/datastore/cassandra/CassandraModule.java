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

package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.inject.*;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;

public class CassandraModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraModule.class);

	public static final String CASSANDRA_AUTH_MAP = "cassandra.auth.map";
	public static final String CASSANDRA_HECTOR_MAP = "cassandra.hector.map";
	public static final String AUTH_PREFIX = "kairosdb.datastore.cassandra.auth.";
	public static final String HECTOR_PREFIX = "kairosdb.datastore.cassandra.hector.";

	private Map<String, String> m_authMap = new HashMap<String, String>();
	private Map<String, Object> m_hectorMap = new HashMap<String, Object>();

	public CassandraModule(Properties props)
	{
		for (Object key : props.keySet())
		{
			String strKey = (String)key;

			if (strKey.startsWith(AUTH_PREFIX))
			{
				String consumerKey = strKey.substring(AUTH_PREFIX.length());
				String consumerToken = (String)props.get(key);

				m_authMap.put(consumerKey, consumerToken);
			}
			else if (strKey.startsWith(HECTOR_PREFIX))
			{
				String configKey = strKey.substring(HECTOR_PREFIX.length());
				m_hectorMap.put(configKey, props.get(key));
			}
		}
	}


	@Override
	protected void configure()
	{
		bind(Datastore.class).to(CassandraDatastore.class).in(Scopes.SINGLETON);
		bind(ServiceKeyStore.class).to(CassandraDatastore.class).in(Scopes.SINGLETON);
		bind(CassandraDatastore.class).in(Scopes.SINGLETON);
		bind(CleanRowKeyCache.class).in(Scopes.SINGLETON);
		bind(CassandraConfiguration.class).in(Scopes.SINGLETON);
		//bind(CassandraClient.class).to(CassandraClientImpl.class);
		bind(CassandraClientImpl.class).in(Scopes.SINGLETON);
		bind(BatchStats.class).in(Scopes.SINGLETON);
		bind(RetryPolicy.class).to(KairosRetryPolicy.class);

		bind(new TypeLiteral<Map<String, String>>(){}).annotatedWith(Names.named(CASSANDRA_AUTH_MAP))
				.toInstance(m_authMap);

		/*bind(new TypeLiteral<Map<String, Object>>(){}).annotatedWith(Names.named(CASSANDRA_HECTOR_MAP))
				.toInstance(m_hectorMap);*/


		install(new FactoryModuleBuilder().build(BatchHandlerFactory.class));

		install(new FactoryModuleBuilder().build(DeleteBatchHandlerFactory.class));

		install(new FactoryModuleBuilder().build(CQLBatchFactory.class));
	}

	@Provides
	@Named("keyspace")
	String getKeyspace(CassandraConfiguration configuration)
	{
		return configuration.getKeyspaceName();
	}

	@Provides
	@Singleton
	CassandraClient getCassandraClient(CassandraConfiguration configuration, Injector injector)
	{
		try
		{
			CassandraClientImpl client = injector.getInstance(CassandraClientImpl.class);
			client.init();
			return client;
		}
		catch (Exception e)
		{
			logger.error("Unable to setup cassandra connection to cluster", e);
			throw e;
		}
	}

	@Provides
	@Singleton
	Schema getCassandraSchema(CassandraClient cassandraClient, CassandraConfiguration configuration)
	{
		try
		{
			return new Schema(cassandraClient, configuration.isCreateSchema());
		}
		catch (Exception e)
		{
			logger.error("Unable to setup cassandra schema", e);
			throw e;
		}
	}

	@Provides
	@Singleton
	LoadBalancingPolicy getLoadBalancingPolicy(CassandraClient cassandraClient)
	{
		return cassandraClient.getWriteLoadBalancingPolicy();
	}

	@Provides
	@Singleton
	ConsistencyLevel getWriteConsistencyLevel(CassandraConfiguration configuration)
	{
		return configuration.getDataWriteLevel();
	}

	@Provides
	@Singleton
	Session getCassandraSession(Schema schema)
	{
		return schema.getSession();
	}


	@Provides
	@Singleton
	DataCache<DataPointsRowKey> getRowKeyCache(CassandraConfiguration configuration)
	{
		return new DataCache<>(configuration.getRowKeyCacheSize());
	}

	@Provides
	@Singleton
	DataCache<String> getMetricNameCache(CassandraConfiguration configuration)
	{
		return new DataCache<>(configuration.getStringCacheSize());
	}

	public interface BatchHandlerFactory
	{
		BatchHandler create(List<DataPointEvent> events, EventCompletionCallBack callBack,
				boolean fullBatch);
	}

	public interface DeleteBatchHandlerFactory
	{
		DeleteBatchHandler create(String metricName, SortedMap<String, String> tags,
				List<DataPoint> dataPoints, EventCompletionCallBack callBack);
	}

	public interface CQLBatchFactory
	{
		CQLBatch create();
	}


}
