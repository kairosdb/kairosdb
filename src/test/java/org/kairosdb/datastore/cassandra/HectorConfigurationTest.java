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
package org.kairosdb.datastore.cassandra;

import me.prettyprint.cassandra.connection.DynamicLoadBalancingPolicy;
import me.prettyprint.cassandra.connection.LoadBalancingPolicy;
import me.prettyprint.cassandra.connection.RoundRobinBalancingPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class HectorConfigurationTest
{
	@Test
	public void test_defaults() throws NoSuchFieldException, IllegalAccessException
	{
		HectorConfiguration hectorConfiguration = new HectorConfiguration("host");
		CassandraHostConfigurator config = hectorConfiguration.getConfiguration();

		assertThat(getIntFieldValue(config, "maxActive"), equalTo(50));
		assertThat(getLongFieldValue(config, "maxWaitTimeWhenExhausted"), equalTo(-1L));
		assertThat(config.getUseSocketKeepalive(), equalTo(false));
		assertThat(getIntFieldValue(config, "cassandraThriftSocketTimeout"), equalTo(0));
		assertThat(getBooleanFieldValue(config, "retryDownedHosts"), equalTo(true));
		assertThat(getIntFieldValue(config, "retryDownedHostsDelayInSeconds"), equalTo(10));
		assertThat(getIntFieldValue(config, "retryDownedHostsQueueSize"), equalTo(-1));
		assertThat(getBooleanFieldValue(config, "autoDiscoverHosts"), equalTo(false));
		assertThat(getIntFieldValue(config, "autoDiscoveryDelayInSeconds"), equalTo(30));
		assertThat(getListFieldValue(config, "autoDiscoveryDataCenters"), equalTo(null));
		assertThat(getBooleanFieldValue(config, "runAutoDiscoveryAtStartup"), equalTo(false));
		assertThat(getBooleanFieldValue(config, "useHostTimeoutTracker"), equalTo(false));
		assertThat(getIntFieldValue(config, "maxFrameSize"), equalTo(2147483647));
		assertThat(getPolicyFieldValue(config, "loadBalancingPolicy").getClass().getName(), equalTo(RoundRobinBalancingPolicy.class.getName()));
		assertThat(getIntFieldValue(config, "hostTimeoutCounter"), equalTo(10));
		assertThat(getIntFieldValue(config, "hostTimeoutWindow"), equalTo(500));
		assertThat(getIntFieldValue(config, "hostTimeoutSuspensionDurationInSeconds"), equalTo(10));
		assertThat(getIntFieldValue(config, "hostTimeoutUnsuspendCheckDelay"), equalTo(10));
		assertThat(getLongFieldValue(config, "maxConnectTimeMillis"), equalTo(-1L));
		assertThat(getLongFieldValue(config, "maxLastSuccessTimeMillis"), equalTo(-1L));
	}

	@Test
	public void test_setValues() throws NoSuchFieldException, IllegalAccessException
	{
		HectorConfiguration hectorConfiguration = new HectorConfiguration("host");
		hectorConfiguration.setMaxActive(100);
		hectorConfiguration.setMaxWaitTimeWhenExhausted(101);
		hectorConfiguration.setUseSocketKeepalive(true);
		hectorConfiguration.setCassandraThriftSocketTimeout(102);
		hectorConfiguration.setRetryDownedHosts(false);
		hectorConfiguration.setRetryDownedHostsDelayInSeconds(103);
		hectorConfiguration.setRetryDownedHostsQueueSize(104);
		hectorConfiguration.setAutoDiscoverHosts(true);
		hectorConfiguration.setAutoDiscoveryDelayInSeconds(105);
		hectorConfiguration.setAutoDiscoveryDataCenters(Arrays.asList("foo", "bar"));
		hectorConfiguration.setRunAutoDiscoveryAtStartup(true);
		hectorConfiguration.setUseHostTimeoutTracker(true);
		hectorConfiguration.setMaxFrameSize(106);
		hectorConfiguration.setLoadBalancingPolicy("dynamic");
		hectorConfiguration.setHostTimeOutCounter(107);
		hectorConfiguration.setHostTimeoutWindow(108);
		hectorConfiguration.setHostTimeOutSuspensionDurationInSeconds(109);
		hectorConfiguration.setHostTimeOutUnsuspendCheckDelay(110);
		hectorConfiguration.setMaxConnectTimeMillis(111L);
		hectorConfiguration.setMaxLastSuccessTimeMillis(112L);

		CassandraHostConfigurator config = hectorConfiguration.getConfiguration();

		assertThat(getIntFieldValue(config, "maxActive"), equalTo(100));
		assertThat(getLongFieldValue(config, "maxWaitTimeWhenExhausted"), equalTo(101L));
		assertThat(config.getUseSocketKeepalive(), equalTo(true));
		assertThat(getIntFieldValue(config, "cassandraThriftSocketTimeout"), equalTo(102));
		assertThat(getBooleanFieldValue(config, "retryDownedHosts"), equalTo(false));
		assertThat(getIntFieldValue(config, "retryDownedHostsDelayInSeconds"), equalTo(103));
		assertThat(getIntFieldValue(config, "retryDownedHostsQueueSize"), equalTo(104));
		assertThat(getBooleanFieldValue(config, "autoDiscoverHosts"), equalTo(true));
		assertThat(getIntFieldValue(config, "autoDiscoveryDelayInSeconds"), equalTo(105));
		assertThat(getListFieldValue(config, "autoDiscoveryDataCenters"), equalTo(Arrays.asList("foo", "bar")));
		assertThat(getBooleanFieldValue(config, "runAutoDiscoveryAtStartup"), equalTo(true));
		assertThat(getBooleanFieldValue(config, "useHostTimeoutTracker"), equalTo(true));
		assertThat(getIntFieldValue(config, "maxFrameSize"), equalTo(106));
		assertThat(getPolicyFieldValue(config, "loadBalancingPolicy").getClass().getName(), equalTo(DynamicLoadBalancingPolicy.class.getName()));
		assertThat(getIntFieldValue(config, "hostTimeoutCounter"), equalTo(107));
		assertThat(getIntFieldValue(config, "hostTimeoutWindow"), equalTo(108));
		assertThat(getIntFieldValue(config, "hostTimeoutSuspensionDurationInSeconds"), equalTo(109));
		assertThat(getIntFieldValue(config, "hostTimeoutUnsuspendCheckDelay"), equalTo(110));
		assertThat(getLongFieldValue(config, "maxConnectTimeMillis"), equalTo(111L));
		assertThat(getLongFieldValue(config, "maxLastSuccessTimeMillis"), equalTo(112L));
	}


	private static int getIntFieldValue(Object config, String fieldName) throws NoSuchFieldException, IllegalAccessException
	{
		Field field = config.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.getInt(config);
	}

	private static long getLongFieldValue(Object config, String fieldName) throws NoSuchFieldException, IllegalAccessException
	{
		Field field = config.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.getLong(config);
	}

	private static boolean getBooleanFieldValue(Object config, String fieldName) throws NoSuchFieldException, IllegalAccessException
	{
		Field field = config.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.getBoolean(config);
	}

	@SuppressWarnings("unchecked")
	private static List<String> getListFieldValue(Object config, String fieldName) throws NoSuchFieldException, IllegalAccessException
	{
		Field field = config.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return (List<String>) field.get(config);
	}

	private static LoadBalancingPolicy getPolicyFieldValue(Object config, String fieldName) throws NoSuchFieldException, IllegalAccessException
	{
		Field field = config.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return (LoadBalancingPolicy) field.get(config);
	}

}

