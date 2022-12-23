package org.kairosdb.core.reporting;

import com.google.inject.Inject;
import org.kairosdb.metrics4j.MetricSourceManager;

import java.util.Collections;


public class JavaMonitor
{
	private Runtime m_runtime;

	@Inject
	public JavaMonitor()
	{
		m_runtime = Runtime.getRuntime();
		MetricSourceManager.addSource(this.getClass().getName(), "maxMemory", Collections.emptyMap(), "JVM max memory", () -> m_runtime.maxMemory());
		MetricSourceManager.addSource(this.getClass().getName(), "freeMemory", Collections.emptyMap(), "JVM free memory", () -> m_runtime.freeMemory());
		MetricSourceManager.addSource(this.getClass().getName(), "totalMemory", Collections.emptyMap(), "JVM total memory", () -> m_runtime.totalMemory());
	}
}
