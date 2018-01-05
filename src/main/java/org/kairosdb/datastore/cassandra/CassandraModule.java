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
import com.google.inject.*;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.kairosdb.core.KairosConfig;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.*;

public class CassandraModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraModule.class);

	public static final String CASSANDRA_AUTH_MAP = "cassandra.auth.map";
	public static final String CASSANDRA_HECTOR_MAP = "cassandra.hector.map";
	public static final String AUTH_PREFIX = "kairosdb.datastore.cassandra.auth.";
	public static final String HECTOR_PREFIX = "kairosdb.datastore.cassandra.hector.";

	private Map<String, String> m_authMap = new HashMap<String, String>();

	public CassandraModule(KairosConfig props)
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

	@Provides
	@Singleton
	@Named("write_cluster")
	ClusterConnection getWriteCluster(CassandraConfiguration configuration)
	{
		try
		{
			CassandraClient client = new CassandraClientImpl(configuration.getWriteCluster());
			return new ClusterConnection(client);
		}
		catch (Exception e)
		{
<<<<<<< HEAD
			logger.error("Error building write cluster", e);
=======
			logger.error("Unable to setup cassandra connection to cluster", e);
>>>>>>> develop
			throw e;
		}


	}

	@Provides
	@Singleton
	@Named("meta_cluster")
	ClusterConnection getMetaCluster(CassandraConfiguration configuration)
			throws Exception
	{
		try
		{
			CassandraClient client = new CassandraClientImpl(configuration.getMetaCluster());
			return new ClusterConnection(client);
		}
		catch (Exception e)
		{
<<<<<<< HEAD
			logger.error("Error building meta cluster", e);
=======
			logger.error("Unable to setup cassandra schema", e);
>>>>>>> develop
			throw e;
		}
	}

	@Provides
	@Singleton
	List<ClusterConnection> getReadClusters(CassandraConfiguration configuration)
	{
		ImmutableList.Builder<ClusterConnection> clusters = new ImmutableList.Builder<>();

		try
		{
			for (ClusterConfiguration clusterConfiguration : configuration.getReadClusters())
			{
				CassandraClient client = new CassandraClientImpl(clusterConfiguration);
				clusters.add(new ClusterConnection(client));
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
