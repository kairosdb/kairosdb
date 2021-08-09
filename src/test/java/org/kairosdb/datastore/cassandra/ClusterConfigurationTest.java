package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.Test;
import org.kairosdb.core.KairosConfig;
import org.kairosdb.core.KairosRootConfig;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Map;

import static org.assertj.guava.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.kairosdb.core.KairosConfig.DATE_TIME_FORMAT;

public class ClusterConfigurationTest
{
	private ClusterConfiguration setupCluster(Map<String, Object> data) throws ParseException
	{
		KairosRootConfig rootConfig = new KairosRootConfig();
		rootConfig.load(data);

		ClusterConfiguration config = new ClusterConfiguration(rootConfig);

		return config;
	}

	private ClusterConfiguration setupCluster(String data) throws ParseException, IOException
	{
		KairosRootConfig rootConfig = new KairosRootConfig();

		try (InputStream is = IOUtils.toInputStream(data, "UTF-8"))
		{
			rootConfig.load(is, KairosConfig.ConfigFormat.HOCON);
		}

		ClusterConfiguration config = new ClusterConfiguration(rootConfig);

		return config;
	}

	@Test(expected = IllegalStateException.class)
	public void test_startTimeAfterEndTime() throws ParseException
	{
		setupCluster(ImmutableMap.of("start_time", "2002-01-01T12:00-0700", "end_time", "2001-01-01T12:00-0700"));
	}

	@Test
	public void test_queryBeforeClusterTime() throws ParseException
	{
		ClusterConfiguration cluster = setupCluster(ImmutableMap.of("start_time", "2002-01-01T12:00-0700", "end_time", "2003-01-01T12:00-0700"));

		assertFalse(cluster.containRange(DATE_TIME_FORMAT.parse("2000-01-01T12:00-0700").getTime(),
				DATE_TIME_FORMAT.parse("2001-01-01T12:00-0700").getTime()));
	}

	@Test
	public void test_queryAfterClusterTime() throws ParseException
	{
		ClusterConfiguration cluster = setupCluster(ImmutableMap.of("start_time", "2002-01-01T12:00-0700", "end_time", "2003-01-01T12:00-0700"));

		assertFalse(cluster.containRange(DATE_TIME_FORMAT.parse("2004-01-01T12:00-0700").getTime(),
				DATE_TIME_FORMAT.parse("2005-01-01T12:00-0700").getTime()));
	}

	@Test
	public void test_queryOverlapClusterStartTime() throws ParseException
	{
		ClusterConfiguration cluster = setupCluster(ImmutableMap.of("start_time", "2002-01-01T12:00-0700", "end_time", "2003-01-01T12:00-0700"));

		assertTrue(cluster.containRange(DATE_TIME_FORMAT.parse("2000-01-01T12:00-0700").getTime(),
				DATE_TIME_FORMAT.parse("2002-02-01T12:00-0700").getTime()));
	}

	@Test
	public void test_queryOverlapClusterEndTime() throws ParseException
	{
		ClusterConfiguration cluster = setupCluster(ImmutableMap.of("start_time", "2002-01-01T12:00-0700",
				"end_time", "2003-01-01T12:00-0700"));

		assertTrue(cluster.containRange(DATE_TIME_FORMAT.parse("2002-12-01T12:00-0700").getTime(),
				DATE_TIME_FORMAT.parse("2003-02-01T12:00-0700").getTime()));
	}

	@Test
	public void test_queryOverlapEntireClusterTime() throws ParseException
	{
		ClusterConfiguration cluster = setupCluster(ImmutableMap.of("start_time", "2002-01-01T12:00-0700",
				"end_time", "2003-01-01T12:00-0700"));

		assertTrue(cluster.containRange(DATE_TIME_FORMAT.parse("2000-12-01T12:00-0700").getTime(),
				DATE_TIME_FORMAT.parse("2003-02-01T12:00-0700").getTime()));
	}

	@Test
	public void test_queryWithinClusterTime() throws ParseException
	{
		ClusterConfiguration cluster = setupCluster(ImmutableMap.of("start_time", "2002-01-01T12:00-0700",
				"end_time", "2003-01-01T12:00-0700"));

		assertTrue(cluster.containRange(DATE_TIME_FORMAT.parse("2002-02-01T12:00-0700").getTime(),
				DATE_TIME_FORMAT.parse("2002-03-01T12:00-0700").getTime()));
	}


	@Test
	public void test_tagIndex_list() throws IOException, ParseException
	{
		String config = "tag_indexed_row_key_lookup_metrics: [key1, key2]";

		ClusterConfiguration clusterConfiguration = setupCluster(config);

		assertThat(clusterConfiguration.getTagIndexedMetrics()).containsAllEntriesOf(ImmutableMultimap.of(
				"key1", "*", "key2", "*"));
	}

	@Test
	public void test_discoverConfigLoading() throws IOException, ParseException
	{
		String config = "tag_indexed_row_key_lookup_metrics: {key1: [], key2: [value1, value2]}";

		ClusterConfiguration clusterConfiguration = setupCluster(config);

		assertThat(clusterConfiguration.getTagIndexedMetrics()).containsAllEntriesOf(ImmutableMultimap.of(
				"key1", "*", "key2", "value1", "key2", "value2"));
	}
}
