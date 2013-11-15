/*
 * Copyright 2013 Proofpoint Inc.
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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.reporting.KairosMetricReporter;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class MonitorFilter implements Filter, KairosMetricReporter
{
	private final String hostname;
	private final ConcurrentMap<String, AtomicInteger> counterMap = new ConcurrentHashMap<String, AtomicInteger>();
	private final LongDataPointFactory m_dataPointFactory;

	@Inject
	public MonitorFilter(@Named("HOSTNAME")String hostname, LongDataPointFactory dataPointFactory)
	{
		this.hostname = checkNotNullOrEmpty(hostname);
		m_dataPointFactory = dataPointFactory;
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException
	{
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
	{
		String path = ((HttpServletRequest)servletRequest).getRequestURI();
		int index = path.lastIndexOf('/');
		if (index > -1)
		{
			String resourceName = path.substring(index + 1);
			AtomicInteger counter = counterMap.get(resourceName);
			if (counter == null)
			{
				counter = new AtomicInteger();
				AtomicInteger mapValue = counterMap.putIfAbsent(resourceName, counter);
				counter = (mapValue != null ? mapValue : counter);
			}
			counter.incrementAndGet();
		}

		filterChain.doFilter(servletRequest, servletResponse);
	}

	@Override
	public void destroy()
	{
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		List<DataPointSet> ret = new ArrayList<DataPointSet>();
		for (String resource : counterMap.keySet())
		{
			DataPointSet dps = new DataPointSet("kairosdb.protocol.http_request_count");
			dps.addTag("host", hostname);
			dps.addTag("method", resource);
			dps.addDataPoint(m_dataPointFactory.createDataPoint(now, (long)counterMap.get(resource).getAndSet(0)));

			ret.add(dps);
		}

		return (ret);
	}
}