package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.junit.Test;
import org.kairosdb.core.KairosConfig;
import org.kairosdb.core.KairosRootConfig;

import java.text.ParseException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.*;

public class CassandraConfigurationTest
{
	private ClusterConfiguration setHosts(List<String> hosts) throws ParseException
	{
		KairosRootConfig rootConfig = new KairosRootConfig();
		rootConfig.load(ImmutableMap.of("cql_host_list", hosts));

		ClusterConfiguration config = new ClusterConfiguration(rootConfig);

		return config;
	}

	private CassandraConfiguration loadConfiguration(String file) throws ParseException
	{
		KairosRootConfig rootConfig = new KairosRootConfig();
		rootConfig.load(this.getClass().getClassLoader().getResourceAsStream(file),
				KairosConfig.ConfigFormat.HOCON);

		return new CassandraConfiguration(rootConfig);
	}

	@Test
	public void test_setHostList() throws ParseException
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

	@Test
	public void test_createTableWithConfig() throws ParseException
	{
		CassandraConfiguration config = loadConfiguration("test_createTableWithConfig.conf");
		assertThat(config.getCreateWithConfig("row_key_index")).isEqualTo(" bla bla bla");
		assertThat(config.getCreateWithConfig("string_index")).isEqualTo("");
		assertThat(config.getCreateWithConfig("data_points")).isEqualTo("WITH COMPACT STORAGE");
	}
}
