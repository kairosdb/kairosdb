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
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class CassandraModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraModule.class);

	public static final String CASSANDRA_AUTH_MAP = "cassandra.auth.map";
	public static final String CASSANDRA_HECTOR_MAP = "cassandra.hector.map";
	public static final String AUTH_PREFIX = "kairosdb.datastore.cassandra.auth.";
	public static final String HECTOR_PREFIX = "kairosdb.datastore.cassandra.hector.";

	private Map<String, String> m_authMap = new HashMap<String, String>();

	public CassandraModule(KairosRootConfig props)
	{
		for (String key : props)
		{
			if (key.startsWith(AUTH_PREFIX))
			{
				String consumerKey = key.substring(AUTH_PREFIX.length());
				String consumerToken = props.getProperty(key);

				m_authMap.put(consumerKey, consumerToken);
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
		//bind(CassandraClientImpl.class).in(Scopes.SINGLETON);
		bind(BatchStats.class).in(Scopes.SINGLETON);

		bind(new TypeLiteral<Map<String, String>>(){}).annotatedWith(Names.named(CASSANDRA_AUTH_MAP))
				.toInstance(m_authMap);

		/*bind(new TypeLiteral<Map<String, Object>>(){}).annotatedWith(Names.named(CASSANDRA_HECTOR_MAP))
				.toInstance(m_hectorMap);*/


		install(new FactoryModuleBuilder().build(BatchHandlerFactory.class));

		install(new FactoryModuleBuilder().build(DeleteBatchHandlerFactory.class));

		install(new FactoryModuleBuilder().build(CQLBatchFactory.class));

		install(new FactoryModuleBuilder().build(CQLFilteredRowKeyIteratorFactory.class));
	}

	/*@Provides
	@Named("keyspace")
	String getKeyspace(CassandraConfiguration configuration)
	{
		return configuration.getKeyspaceName();
	}*/

	/**
	 Bind classes that are specific to the cluster connection
	 @param binder
	 @param config
	 */
	private void bindCassandraClient(Binder binder, ClusterConfiguration config)
	{
		binder.bind(ClusterConfiguration.class).toInstance(config);
		binder.bind(CassandraClient.class).to(CassandraClientImpl.class);
		binder.bindConstant().annotatedWith(Names.named("request_retry_count")).to(config.getRequestRetryCount());
		binder.bindConstant().annotatedWith(Names.named("cluster_name")).to(config.getClusterName());
		binder.bind(KairosRetryPolicy.class);
	}

	private ClusterConnection m_writeCluster;
	private ClusterConnection m_metaCluster;

	private void createClients(CassandraConfiguration configuration, Injector injector)
	{
		if (m_metaCluster != null)
			return;

		ClusterConfiguration writeConfig = configuration.getWriteCluster();
		ClusterConfiguration metaConfig = configuration.getMetaCluster();

		Injector writeInjector = injector.createChildInjector((Module) binder -> bindCassandraClient(binder, writeConfig) );

		CassandraClient writeClient = writeInjector.getInstance(CassandraClient.class);

		if (writeConfig == metaConfig) //No separate meta cluster configuration
		{
			m_metaCluster = m_writeCluster = new ClusterConnection(writeClient, EnumSet.of(
					ClusterConnection.Type.WRITE, ClusterConnection.Type.META));
			m_metaCluster.startup(configuration.isStartAsync());
		}
		else
		{
			m_writeCluster = new ClusterConnection(writeClient, EnumSet.of(
					ClusterConnection.Type.WRITE));
			m_writeCluster.startup(configuration.isStartAsync());

			Injector metaInjector = injector.createChildInjector((Module) binder -> bindCassandraClient(binder, metaConfig) );

			CassandraClient metaClient = metaInjector.getInstance(CassandraClient.class);

			m_metaCluster = new ClusterConnection(metaClient, EnumSet.of(
					ClusterConnection.Type.META));
			m_metaCluster.startup(configuration.isStartAsync());
		}
	}

	@Provides
	@Singleton
	@Named("write_cluster")
	ClusterConnection getWriteCluster(CassandraConfiguration configuration, Injector injector)
	{
		try
		{
			createClients(configuration, injector);
			return m_writeCluster;
		}
		catch (Exception e)
		{
			logger.error("Error building write cluster", e);
			throw e;
		}


	}

	@Provides
	@Singleton
	@Named("meta_cluster")
	ClusterConnection getMetaCluster(CassandraConfiguration configuration, Injector injector)
			throws Exception
	{
		try
		{
			createClients(configuration, injector);
			return m_metaCluster;
		}
		catch (Exception e)
		{
			logger.error("Error building meta cluster", e);
			throw e;
		}
	}

	@Provides
	@Singleton
	List<ClusterConnection> getReadClusters(CassandraConfiguration configuration, Injector injector)
	{
		ImmutableList.Builder<ClusterConnection> clusters = new ImmutableList.Builder<>();

		try
		{
			for (ClusterConfiguration clusterConfiguration : configuration.getReadClusters())
			{
				Injector readInjector = injector.createChildInjector((Module) binder -> bindCassandraClient(binder, clusterConfiguration) );

				CassandraClient client = readInjector.getInstance(CassandraClient.class);

				clusters.add(new ClusterConnection(client, EnumSet.of(ClusterConnection.Type.READ)).startup(configuration.isStartAsync()));
			}
		}
		catch (Exception e)
		{
			logger.error("Error building read cluster", e);
			throw e;
		}

		return clusters.build();
	}

	@Provides
	@Singleton
	LoadBalancingPolicy getLoadBalancingPolicy(@Named("write_cluster")ClusterConnection connection)
	{
		return connection.getLoadBalancingPolicy();
	}

	@Provides
	@Singleton
	ConsistencyLevel getWriteConsistencyLevel(CassandraConfiguration configuration)
	{
		return configuration.getWriteCluster().getWriteConsistencyLevel();
	}

	/*@Provides
	@Singleton
	Session getCassandraSession(ClusterConnection clusterConnection)
	{
		return clusterConnection.getSession();
	}*/


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

	public interface CQLFilteredRowKeyIteratorFactory
	{
		CQLFilteredRowKeyIterator create(ClusterConnection cluster,
				String metricName,
				@Assisted("startTime") long startTime,
				@Assisted("endTime") long endTime,
				SetMultimap<String, String> filterTags) throws DatastoreException;
	}


}
