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
package org.kairosdb.core.reporting;

import com.yammer.metrics.core.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class KairosMetricRegistry extends MetricsRegistry
{
	public static class Tag
	{
		private String name;
		private String value;

		public Tag(String name, String value)
		{
			this.name = name;
			this.value = value;
		}

		public String getName()
		{
			return name;
		}

		public String getValue()
		{
			return value;
		}
	}

	private Map<MetricName, Map<String, String>> metricTagsMap = new HashMap<MetricName, Map<String, String>>();

	public Counter newCounter(MetricName metricName, Tag... tags)
	{
		addTags(metricName, tags);
		return super.newCounter(metricName);
	}

	public <T> Gauge<T> newGauge(MetricName metricName, Gauge<T> metric, Tag... tags)
	{
		addTags(metricName, tags);
		return super.newGauge(metricName, metric);
	}

	public Histogram newHistogram(MetricName metricName, boolean biased, Tag... tags)
	{
		addTags(metricName, tags);
		return super.newHistogram(metricName, biased);
	}

	public Meter newMeter(MetricName metricName, String eventType, TimeUnit unit, Tag... tags)
	{
		addTags(metricName, tags);
		return super.newMeter(metricName, eventType, unit);
	}

	public Timer newTimer(MetricName metricName, TimeUnit durationUnit, TimeUnit rateUnit, Tag... tags)
	{
		addTags(metricName, tags);
		return super.newTimer(metricName, durationUnit, rateUnit);
	}

	public Map<String, String> getTags(MetricName metricName)
	{
		Map<String, String> map = metricTagsMap.get(metricName);
		if (map == null)
			map = Collections.emptyMap();
		return map;
	}

	private void addTags(MetricName metricName, Tag... tags)
	{
		Map<String, String> map = metricTagsMap.get(metricName);
		if (map == null)
		{
			map = new HashMap<String, String>();
			metricTagsMap.put(metricName, map);
		}
		for (Tag tag : tags)
		{
			map.put(tag.getName(), tag.getValue());
		}
	}

	public String getKairosName(MetricName metricName)
	{
		checkNotNull(metricName);
		return metricNameToString(metricName);
	}

	private String metricNameToString(MetricName name)
	{
		final StringBuilder sb = new StringBuilder()
				.append(name.getGroup())
				.append('.')
				.append(name.getType())
				.append('.')
				.append(name.getName());
		return sb.toString();
	}

}