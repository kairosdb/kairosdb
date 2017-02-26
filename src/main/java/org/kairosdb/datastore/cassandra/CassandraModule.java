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

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.kairosdb.core.datastore.Datastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CassandraModule extends AbstractModule
{
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
		bind(CassandraDatastore.class).in(Scopes.SINGLETON);
		bind(CleanRowKeyCache.class).in(Scopes.SINGLETON);
		bind(CassandraConfiguration.class).in(Scopes.SINGLETON);
		bind(CassandraClient.class).to(CassandraClientImpl.class);
		bind(CassandraClientImpl.class).in(Scopes.SINGLETON);

		bind(new TypeLiteral<Map<String, String>>(){}).annotatedWith(Names.named(CASSANDRA_AUTH_MAP))
				.toInstance(m_authMap);

		bind(new TypeLiteral<Map<String, Object>>(){}).annotatedWith(Names.named(CASSANDRA_HECTOR_MAP))
				.toInstance(m_hectorMap);
	}
}
