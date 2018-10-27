package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.KairosRootConfig;
import org.mockito.Mockito;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class RowKeyLookupProviderTest
{
	private ClusterConnection clusterConnection;

	@Before
	public void setUp() throws Exception
	{
		clusterConnection = Mockito.mock(ClusterConnection.class);
	}

	@Test
	public void testWithWildcard()
	{
		RowKeyLookupProvider rowKeyLookupProvider = new RowKeyLookupProvider(clusterConnection, "*");
		RowKeyLookup rowKeyLookup = rowKeyLookupProvider.getRowKeyLookupForMetric("someMetric");

		assertThat(rowKeyLookup, instanceOf(RowKeyLookupProvider.TagIndexedRowKeysTableLookup.class));
	}

	@Test
	public void testWithoutEmptyStringConfig()
	{
		RowKeyLookupProvider rowKeyLookupProvider = new RowKeyLookupProvider(clusterConnection, "");
		RowKeyLookup rowKeyLookup = rowKeyLookupProvider.getRowKeyLookupForMetric("someMetric");

		assertThat(rowKeyLookup, instanceOf(RowKeyLookupProvider.RowKeysTableLookup.class));
	}

	@Test
	public void testWithNullConfig()
	{
		RowKeyLookupProvider rowKeyLookupProvider = new RowKeyLookupProvider(clusterConnection, null);
		RowKeyLookup rowKeyLookup = rowKeyLookupProvider.getRowKeyLookupForMetric("someMetric");

		assertThat(rowKeyLookup, instanceOf(RowKeyLookupProvider.RowKeysTableLookup.class));
	}

	@Test
	public void testWithMetricSetConfig()
	{
		RowKeyLookupProvider rowKeyLookupProvider = new RowKeyLookupProvider(clusterConnection, "metricA, metricB");

		assertThat(
				rowKeyLookupProvider.getRowKeyLookupForMetric("someMetric"),
				instanceOf(RowKeyLookupProvider.RowKeysTableLookup.class));

		assertThat(
				rowKeyLookupProvider.getRowKeyLookupForMetric("metricA"),
				instanceOf(RowKeyLookupProvider.TagIndexedRowKeysTableLookup.class));

		assertThat(
				rowKeyLookupProvider.getRowKeyLookupForMetric("metricB"),
				instanceOf(RowKeyLookupProvider.TagIndexedRowKeysTableLookup.class));
	}

}