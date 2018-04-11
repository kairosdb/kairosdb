package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.junit.Test;
import org.kairosdb.core.KairosConfig;
import org.kairosdb.core.KairosRootConfig;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CassandraConfigurationTest
{
	private ClusterConfiguration setHosts(List<String> hosts)
	{
		KairosRootConfig rootConfig = new KairosRootConfig();
		rootConfig.load(ImmutableMap.of("cql_host_list", hosts));

		ClusterConfiguration config = new ClusterConfiguration(rootConfig);

		return config;
	}

	@Test
	public void test_setHostList()
	{
		ClusterConfiguration config = setHosts(ImmutableList.of("localhost:9000"));
		assertEquals(ImmutableMap.of("localhost", 9000), config.getHostList());

		config = setHosts(ImmutableList.of("localhost:9000", "otherhost:8000"));
		assertEquals(ImmutableMap.of("localhost", 9000,
				"otherhost", 8000), config.getHostList());

		config = setHosts(ImmutableList.of("localhost", "otherhost"));
		assertEquals(ImmutableMap.of("localhost", 9042,
				"otherhost", 9042), config.getHostList());

		config = setHosts(ImmutableList.of("localhost:", "otherhost"));
		assertEquals(ImmutableMap.of("localhost", 9042,
				"otherhost", 9042), config.getHostList());

		config = setHosts(ImmutableList.of("localhost:"));
		assertEquals(ImmutableMap.of("localhost", 9042), config.getHostList());

		config = setHosts(ImmutableList.of("localhost"));
		assertEquals(ImmutableMap.of("localhost", 9042), config.getHostList());
	}
}
