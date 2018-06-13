package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.kairosdb.core.KairosRootConfig;

import java.text.ParseException;
import java.util.Map;

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
}
