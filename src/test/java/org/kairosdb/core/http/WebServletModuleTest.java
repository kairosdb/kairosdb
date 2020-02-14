package org.kairosdb.core.http;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import org.kairosdb.core.KairosRootConfig;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class WebServletModuleTest
{
	@Test
	public void test_ReadingPropertiesQosEnabled()
	{
		KairosRootConfig props = new KairosRootConfig();
		props.load(ImmutableMap.of("kairosdb.qos.url", "/*"));
		props.load(ImmutableMap.of("kairosdb.qos.maxRequests", "5"));
		props.load(ImmutableMap.of("kairosdb.qos.waitMs", "10"));
		props.load(ImmutableMap.of("kairosdb.qos.suspendMs", "-1"));

		WebServletModule module = new WebServletModule(props);

		assertEquals("/*", module.getQosUrl());
		assertEquals("5", module.getQosParams().get("maxRequests"));
		assertEquals("10", module.getQosParams().get("waitMs"));
		assertEquals("-1", module.getQosParams().get("suspendMs"));
	}

	@Test
	public void test_ReadingPropertiesQosDisabled()
	{
		KairosRootConfig props = new KairosRootConfig();
		props.load(ImmutableMap.of("kairosdb.qos.url", ""));
		props.load(ImmutableMap.of("kairosdb.qos.maxRequests", "5"));
		props.load(ImmutableMap.of("kairosdb.qos.waitMs", "10"));
		props.load(ImmutableMap.of("kairosdb.qos.suspendMs", "-1"));

		WebServletModule module = new WebServletModule(props);

		assertNull("QosURL is not null", module.getQosUrl());
		assertNull("Qos params is not null", module.getQosParams());
	}
}
