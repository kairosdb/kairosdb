package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CassandraConfigurationTest
{
	@Test
	public void test_setHostList()
	{
		CassandraConfiguration config = new CassandraConfiguration();

		config.setHostList("localhost:9000");
		assertEquals(ImmutableMap.of("localhost", 9000), config.getHostList());

		config = new CassandraConfiguration();
		config.setHostList("localhost:9000,otherhost:8000");
		assertEquals(ImmutableMap.of("localhost", 9000,
				"otherhost", 8000), config.getHostList());

		config = new CassandraConfiguration();
		config.setHostList("localhost, otherhost");
		assertEquals(ImmutableMap.of("localhost", 9042,
				"otherhost", 9042), config.getHostList());

		config = new CassandraConfiguration();
		config.setHostList("localhost:,otherhost");
		assertEquals(ImmutableMap.of("localhost", 9042,
				"otherhost", 9042), config.getHostList());

		config = new CassandraConfiguration();
		config.setHostList("localhost:");
		assertEquals(ImmutableMap.of("localhost", 9042), config.getHostList());

		config = new CassandraConfiguration();
		config.setHostList("localhost");
		assertEquals(ImmutableMap.of("localhost", 9042), config.getHostList());
	}
}
