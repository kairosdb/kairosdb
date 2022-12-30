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
package org.kairosdb.core.http;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.kairosdb.metrics4j.MetricSourceManager;
import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

public class MonitorFilter implements Filter
{
	private static class PrefixFilter
	{
		private final String prefix;
		private final Function<String, String> filter;
		public PrefixFilter(String prefix, Function<String, String> filter)
		{
			this.prefix = prefix;
			this.filter = filter;
		}
	}
	public static final List<PrefixFilter> PATH_FILTERS = new ArrayList<>();
	static {
		//Add filters in order of usage
		PATH_FILTERS.add(new PrefixFilter("/api/v1/datapoints", s -> s));
		PATH_FILTERS.add(new PrefixFilter("/api/v1/rollups", s -> "/api/v1/rollups"));
	}

	public interface HttpStats
	{
		LongCollector httpRequestCount(@Key("method")String method, @Key("path")String path);
	}

	private static final Logger logger = LoggerFactory.getLogger(MonitorFilter.class);
	private static final HttpStats stats = MetricSourceManager.getSource(HttpStats.class);
    
	private final ConcurrentMap<String, AtomicInteger> counterMap = new ConcurrentHashMap<String, AtomicInteger>();
	private final LongDataPointFactory m_dataPointFactory;

	@Inject
	public MonitorFilter(LongDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException
	{
	}

	public static String getFilteredPath(String path)
	{
		for (PrefixFilter pathFilter : PATH_FILTERS)
		{
			if (path.startsWith(pathFilter.prefix))
			{
				return pathFilter.filter.apply(path);
			}
		}

		return path;
	}

	public static void reportMetric(String method, String path)
	{
		path = getFilteredPath(path);
		stats.httpRequestCount(method, path).put(1);
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
	{
		String path = ((HttpServletRequest)servletRequest).getRequestURI();
		String method = ((HttpServletRequest) servletRequest).getMethod();
		reportMetric(method, path);

		filterChain.doFilter(servletRequest, servletResponse);
	}

	@Override
	public void destroy()
	{
	}

	//@Override
	public List<DataPointSet> getMetrics(long now)
	{
		List<DataPointSet> ret = new ArrayList<DataPointSet>();
		for (String resource : counterMap.keySet())
		{
			//todo this is broken
			DataPointSet dps = new DataPointSet("kairosdb.protocol.http_request_count");
			dps.addTag("method", resource);
			dps.addDataPoint(m_dataPointFactory.createDataPoint(now, (long)counterMap.get(resource).getAndSet(0)));

			ret.add(dps);
		}

		return (ret);
	}
}
