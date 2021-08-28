package org.kairosdb.datastore.cassandra;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.KairosRootConfig;
import org.mockito.Mockito;

import java.text.ParseException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowKeyLookupProviderTest
{
	private CassandraConfiguration m_cassandraConfiguration;
	private CassandraClient m_cassandraClient;
	private EnumSet<ClusterConnection.Type> m_clusterType;

	@Before
	public void setUp() throws Exception
	{
		ClusterConfiguration clusterConfiguration = mock(ClusterConfiguration.class);
		when(clusterConfiguration.getClusterName()).thenReturn("TestCluster");

		m_cassandraClient = mock(CassandraClient.class);
		when(m_cassandraClient.getClusterConfiguration()).thenReturn(clusterConfiguration);

		m_clusterType = EnumSet.of(ClusterConnection.Type.WRITE, ClusterConnection.Type.META);

		m_cassandraConfiguration = mock(CassandraConfiguration.class);
	}

	@Test
	public void testWithWildcard()
	{
		ClusterConnection connection = new ClusterConnection(m_cassandraConfiguration, m_cassandraClient, m_clusterType,
				ImmutableMultimap.of("*", "*"));

		RowKeyLookup rowKeyLookup = connection.getRowKeyLookupForMetric("someMetric");

		assertThat(rowKeyLookup, instanceOf(ClusterConnection.TagIndexedRowKeysTableLookup.class));
	}

	@Test
	public void testWithoutEmptyStringConfig()
	{
		ClusterConnection connection = new ClusterConnection(m_cassandraConfiguration, m_cassandraClient, m_clusterType,
				ImmutableMultimap.of("", ""));

		RowKeyLookup rowKeyLookup = connection.getRowKeyLookupForMetric("someMetric");

		assertThat(rowKeyLookup, instanceOf(ClusterConnection.RowKeysTableLookup.class));
	}

	@Test
	public void testWithEmptySet()
	{
		ClusterConnection connection = new ClusterConnection(m_cassandraConfiguration, m_cassandraClient, m_clusterType,
				ImmutableMultimap.of());

		RowKeyLookup rowKeyLookup = connection.getRowKeyLookupForMetric("someMetric");

		assertThat(rowKeyLookup, instanceOf(ClusterConnection.RowKeysTableLookup.class));
	}

	@Test
	public void testWithMetricSetConfig()
	{
		ClusterConnection connection = new ClusterConnection(m_cassandraConfiguration, m_cassandraClient, m_clusterType,
				ImmutableMultimap.of("metricA", "*", "metricB", "*"));

		RowKeyLookup rowKeyLookup = connection.getRowKeyLookupForMetric("someMetric");

		assertThat(
				connection.getRowKeyLookupForMetric("someMetric"),
				instanceOf(ClusterConnection.RowKeysTableLookup.class));

		assertThat(
				connection.getRowKeyLookupForMetric("metricA"),
				instanceOf(ClusterConnection.TagIndexedRowKeysTableLookup.class));

		assertThat(
				connection.getRowKeyLookupForMetric("metricB"),
				instanceOf(ClusterConnection.TagIndexedRowKeysTableLookup.class));
	}

}