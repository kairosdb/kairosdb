//
//  HectorConfiguration.java
//
// Copyright 2016, KairosDB Authors
//        
package org.kairosdb.datastore.cassandra;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import me.prettyprint.cassandra.connection.DynamicLoadBalancingPolicy;
import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.connection.RoundRobinBalancingPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;

import java.util.List;

public class HectorConfiguration
{
	private static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.host_list";

	private static final String MAX_ACTIVE = "kairosdb.datastore.cassandra.hector.maxActive";
	private static final String MAX_WAIT_TIME_WHEN_EXHAUSTED = "kairosdb.datastore.cassandra.hector.maxWaitTimeWhenExhausted";
	private static final String USE_SOCKET_KEEP_ALIVE = "kairosdb.datastore.cassandra.hector.useSocketKeepalive";
	private static final String CASSANDRA_THRIFT_SOCKET_TIMEOUT = "kairosdb.datastore.cassandra.hector.cassandraThriftSocketTimeout";
	private static final String RETRY_DOWNED_HOSTS = "kairosdb.datastore.cassandra.hector.retryDownedHosts";
	private static final String RETRY_DOWNED_HOSTS_DELAY_IN_SECONDS = "kairosdb.datastore.cassandra.hector.retryDownedHostsDelayInSeconds";
	private static final String RETRY_DOWNED_HOSTS_QUEUE_SIZE = "kairosdb.datastore.cassandra.hector.retryDownedHostsQueueSize";
	private static final String AUTO_DISCOVER_HOSTS = "kairosdb.datastore.cassandra.hector.autoDiscoverHosts";
	private static final String AUTO_DISCOVER_DELAY_IN_SECONDS = "kairosdb.datastore.cassandra.hector.autoDiscoveryDelayInSeconds";
	private static final String AUTO_DISCOVERY_DATA_CENTERS = "kairosdb.datastore.cassandra.hector.autoDiscoveryDataCenters";
	private static final String RUN_AUTO_DISCOVERY_AT_STARTUP = "kairosdb.datastore.cassandra.hector.runAutoDiscoveryAtStartup";
	private static final String USE_HOST_TIME_OUT_TRACKER = "kairosdb.datastore.cassandra.hector.useHostTimeoutTracker";
	private static final String MAX_FRAME_SIZE = "kairosdb.datastore.cassandra.hector.maxFrameSize";
	private static final String LOAD_BALANCING_POLICY = "kairosdb.datastore.cassandra.hector.loadBalancingPolicy";
	private static final String HOST_TIME_OUT_COUNTER = "kairosdb.datastore.cassandra.hector.hostTimeoutCounter";
	private static final String HOST_TIME_OUT_WINDOW = "kairosdb.datastore.cassandra.hector.hostTimeoutWindow";
	private static final String HOST_TIME_OUT_SUSPENSION_DURATION_IN_SECONDS = "kairosdb.datastore.cassandra.hector.hostTimeoutSuspensionDurationInSeconds";
	private static final String HOST_TIME_OUT_UNSUSPEND_CHECK_DELAY = "kairosdb.datastore.cassandra.hector.hostTimeoutUnsuspendCheckDelay";
	private static final String MAX_CONNECT_TIME_MILLIS = "kairosdb.datastore.cassandra.hector.maxConnectTimeMillis";
	private static final String MAX_LAST_SUCCESS_TIME_MILLIS = "kairosdb.datastore.cassandra.hector.maxLastSuccessTimeMillis";

	private CassandraHostConfigurator hostConfig;


	@Inject
	public HectorConfiguration(@Named(HOST_LIST_PROPERTY) String cassandraHostList)
	{
		hostConfig = new CassandraHostConfigurator(cassandraHostList);
	}

	public CassandraHostConfigurator getConfiguration()
	{
		return hostConfig;
	}

	@Inject(optional = true)
	public void setMaxActive(@Named(MAX_ACTIVE) int maxActive)
	{
		hostConfig.setMaxActive(maxActive);
	}

	@Inject(optional = true)
	public void setMaxWaitTimeWhenExhausted(@Named(MAX_WAIT_TIME_WHEN_EXHAUSTED) long maxWaitTimeWhenExhausted)
	{
		hostConfig.setMaxWaitTimeWhenExhausted(maxWaitTimeWhenExhausted);
	}

	@Inject(optional = true)
	public void setUseSocketKeepalive(@Named(USE_SOCKET_KEEP_ALIVE) boolean useSocketKeepalive)
	{
		hostConfig.setUseSocketKeepalive(useSocketKeepalive);
	}

	@Inject(optional = true)
	public void setCassandraThriftSocketTimeout(@Named(CASSANDRA_THRIFT_SOCKET_TIMEOUT) int cassandraThriftSocketTimeout)
	{
		hostConfig.setCassandraThriftSocketTimeout(cassandraThriftSocketTimeout);
	}

	@Inject(optional = true)
	public void setRetryDownedHosts(@Named(RETRY_DOWNED_HOSTS) boolean retryDownedHosts)
	{
		hostConfig.setRetryDownedHosts(retryDownedHosts);
	}

	@Inject(optional = true)
	public void setRetryDownedHostsDelayInSeconds(@Named(RETRY_DOWNED_HOSTS_DELAY_IN_SECONDS)
	                                              int retryDownedHostsDelayInSeconds)
	{
		hostConfig.setRetryDownedHostsDelayInSeconds(retryDownedHostsDelayInSeconds);
	}

	@Inject(optional = true)
	public void setRetryDownedHostsQueueSize(@Named(RETRY_DOWNED_HOSTS_QUEUE_SIZE) int retryDownedHostsQueueSize)
	{
		hostConfig.setRetryDownedHostsQueueSize(retryDownedHostsQueueSize);
	}

	@Inject(optional = true)
	public void setAutoDiscoverHosts(@Named(AUTO_DISCOVER_HOSTS) boolean autoDiscoverHosts)
	{
		hostConfig.setAutoDiscoverHosts(autoDiscoverHosts);
	}

	@Inject(optional = true)
	public void setAutoDiscoveryDelayInSeconds(@Named(AUTO_DISCOVER_DELAY_IN_SECONDS) int autoDiscoveryDelayInSeconds)
	{
		hostConfig.setAutoDiscoveryDelayInSeconds(autoDiscoveryDelayInSeconds);
	}

	@Inject(optional = true)
	public void setAutoDiscoveryDataCenters(@Named(AUTO_DISCOVERY_DATA_CENTERS) List<String> autoDiscoveryDataCenters)
	{
		hostConfig.setAutoDiscoveryDataCenter(autoDiscoveryDataCenters);
	}

	@Inject(optional = true)
	public void setRunAutoDiscoveryAtStartup(@Named(RUN_AUTO_DISCOVERY_AT_STARTUP) boolean runAutoDiscoveryAtStartup)
	{
		hostConfig.setRunAutoDiscoveryAtStartup(runAutoDiscoveryAtStartup);
	}

	@Inject(optional = true)
	public void setUseHostTimeoutTracker(@Named(USE_HOST_TIME_OUT_TRACKER) boolean useHostTimeoutTracker)
	{
		hostConfig.setUseHostTimeoutTracker(useHostTimeoutTracker);
	}

	@Inject(optional = true)
	public void setMaxFrameSize(@Named(MAX_FRAME_SIZE) int maxFrameSize)
	{
		hostConfig.setMaxFrameSize(maxFrameSize);
	}

	@Inject(optional = true)
	public void setLoadBalancingPolicy(@Named(LOAD_BALANCING_POLICY) String loadBalancingPolicy)
	{
		if (loadBalancingPolicy.equals("dynamic"))
			hostConfig.setLoadBalancingPolicy(new DynamicLoadBalancingPolicy());
		else if (loadBalancingPolicy.equals("leastActive"))
			hostConfig.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
		else if (loadBalancingPolicy.equals("roundRobin"))
			hostConfig.setLoadBalancingPolicy(new RoundRobinBalancingPolicy());
		else
			throw new IllegalArgumentException("Invalid load balancing policy. Must be dynamic, leastActive, or roundRobin");
	}

	@Inject(optional = true)
	public void setHostTimeOutCounter(@Named(HOST_TIME_OUT_COUNTER) int hostTimeoutCounter)
	{
		hostConfig.setHostTimeoutCounter(hostTimeoutCounter);
	}

	@Inject(optional = true)
	public void setHostTimeoutWindow(@Named(HOST_TIME_OUT_WINDOW) int hostTimeoutWindow)
	{
		hostConfig.setHostTimeoutWindow(hostTimeoutWindow);
	}

	@Inject(optional = true)
	public void setHostTimeOutSuspensionDurationInSeconds(@Named(HOST_TIME_OUT_SUSPENSION_DURATION_IN_SECONDS)
	                                                      int hostTimeoutSuspensionDurationInSeconds)
	{
		hostConfig.setHostTimeoutSuspensionDurationInSeconds(hostTimeoutSuspensionDurationInSeconds);
	}

	@Inject(optional = true)
	public void setHostTimeOutUnsuspendCheckDelay(@Named(HOST_TIME_OUT_UNSUSPEND_CHECK_DELAY)
	                                              int hostTimeoutUnsuspendCheckDelay)
	{
		hostConfig.setHostTimeoutUnsuspendCheckDelay(hostTimeoutUnsuspendCheckDelay);
	}

	@Inject(optional = true)
	public void setMaxConnectTimeMillis(@Named(MAX_CONNECT_TIME_MILLIS) long maxConnectTimeMillis)
	{
		hostConfig.setMaxConnectTimeMillis(maxConnectTimeMillis);
	}

	@Inject(optional = true)
	public void setMaxLastSuccessTimeMillis(@Named(MAX_LAST_SUCCESS_TIME_MILLIS) long maxLastSuccessTimeMillis)
	{
		hostConfig.setMaxLastSuccessTimeMillis(maxLastSuccessTimeMillis);
	}
}