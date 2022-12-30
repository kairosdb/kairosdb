package org.kairosdb.core.http;


import org.junit.Test;
import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.collectors.LongCollector;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MonitorFilterTest
{
	@Test
	public void testFilterMetrics_metricnames()
	{
		LongCollector mockCollector = mock(LongCollector.class);
		MetricSourceManager.setCollectorForSource(mockCollector, MonitorFilter.HttpStats.class).httpRequestCount("GET", "/api/v1/metricnames");

		MonitorFilter.reportMetric("GET", "/api/v1/metricnames");

		verify(mockCollector).put(1);
	}

	@Test
	public void testFilterMetrics_rollup_status()
	{
		LongCollector mockCollector = mock(LongCollector.class);
		MetricSourceManager.setCollectorForSource(mockCollector, MonitorFilter.HttpStats.class).httpRequestCount("GET", "/api/v1/rollups");

		MonitorFilter.reportMetric("GET", "/api/v1/rollups/status/20184166-a598-467b-90cf-9f775e75953b");

		verify(mockCollector).put(1);
	}

	@Test
	public void testFilterMetrics_datapoints()
	{
		LongCollector mockCollector = mock(LongCollector.class);
		MetricSourceManager.setCollectorForSource(mockCollector, MonitorFilter.HttpStats.class).httpRequestCount("GET", "/api/v1/datapoints/query/tags");

		MonitorFilter.reportMetric("GET", "/api/v1/datapoints/query/tags");

		verify(mockCollector).put(1);
	}
}