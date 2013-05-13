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

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.ToggleGauge;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.kairosdb.core.reporting.KairosMetricRegistry.Tag;

public class KairosMetricRegistryTest
{
	private KairosMetricRegistry registry;

	@Before
	public void setup()
	{
		registry = new KairosMetricRegistry();
	}

	@Test
	public void test_tags()
	{
		registry.newCounter(new MetricName("foo", "bar", "one"), new Tag("tag1", "value1"));
		registry.newGauge(new MetricName("foo", "bar", "two"), new ToggleGauge(), new Tag("tag2", "value2"));
		registry.newHistogram(new MetricName("foo", "bar", "three"), false, new Tag("tag3", "value3"));
		registry.newMeter(new MetricName("foo", "bar", "four"), "event", TimeUnit.MILLISECONDS, new Tag("tag4", "value4"));
		registry.newTimer(new MetricName("foo", "bar", "five"), TimeUnit.DAYS, TimeUnit.MILLISECONDS, new Tag("tag5", "value5"), new Tag("tag6", "value6"));

		assertThat(registry.getTags(new MetricName("foo", "bar", "one")), equalTo(createMap(new Tag("tag1", "value1"))));
		assertThat(registry.getTags(new MetricName("foo", "bar", "two")), equalTo(createMap(new Tag("tag2", "value2"))));
		assertThat(registry.getTags(new MetricName("foo", "bar", "three")), equalTo(createMap(new Tag("tag3", "value3"))));
		assertThat(registry.getTags(new MetricName("foo", "bar", "five")), equalTo(createMap(new Tag("tag5", "value5"), new Tag("tag6", "value6"))));
	}

	@Test(expected = NullPointerException.class)
	public void test_getKairosName_NullNameInvalid()
	{
		KairosMetricRegistry registry = new KairosMetricRegistry();

		registry.getKairosName(null);
	}

	@Test
	public void test_getKairosName()
	{
		assertThat(registry.getKairosName(new MetricName("foo", "bar", "this", "that")), equalTo("foo.bar.this"));
		assertThat(registry.getKairosName(new MetricName("foo", "bar", "this")), equalTo("foo.bar.this"));
	}

	private Map<String, String> createMap(Tag... tags){
		Map<String, String> map = new HashMap<String, String>();
		for (Tag tag : tags)
		{
			map.put(tag.getName(), tag.getValue());
		}
		return map;
	}
}