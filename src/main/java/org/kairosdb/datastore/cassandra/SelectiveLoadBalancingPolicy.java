package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;

import java.util.Collection;
import java.util.Iterator;


/**
 This class holds two different load balancing policies.  One for queries and one
 for ingesting data.  This then returns a query plan based on if the statement is a
 batch statement (for ingest) or not.

 The purpose is so inserts will not shuffle replicas so batching can be done efficiently
 but queries will shuffle replicas
 */
public class SelectiveLoadBalancingPolicy implements LoadBalancingPolicy
{
	private final LoadBalancingPolicy m_queryPolicy;
	private final LoadBalancingPolicy m_writePolicy;

	public SelectiveLoadBalancingPolicy(LoadBalancingPolicy queryPolicy, LoadBalancingPolicy writePolicy)
	{
		m_queryPolicy = queryPolicy;
		m_writePolicy = writePolicy;
	}

	@Override
	public void init(Cluster cluster, Collection<Host> hosts)
	{
		m_queryPolicy.init(cluster, hosts);
		m_writePolicy.init(cluster, hosts);
	}

	@Override
	public HostDistance distance(Host host)
	{
		return m_writePolicy.distance(host);
	}

	@Override
	public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement)
	{
		if (statement instanceof BatchStatement)
		{
			return m_writePolicy.newQueryPlan(loggedKeyspace, statement);
		}
		else
		{
			return m_queryPolicy.newQueryPlan(loggedKeyspace, statement);
		}
	}

	@Override
	public void onAdd(Host host)
	{
		m_queryPolicy.onAdd(host);
		m_writePolicy.onAdd(host);
	}

	@Override
	public void onUp(Host host)
	{
		m_queryPolicy.onUp(host);
		m_writePolicy.onUp(host);
	}

	@Override
	public void onDown(Host host)
	{
		m_queryPolicy.onDown(host);
		m_writePolicy.onDown(host);
	}

	@Override
	public void onRemove(Host host)
	{
		m_queryPolicy.onRemove(host);
		m_writePolicy.onRemove(host);
	}

	@Override
	public void close()
	{
		m_queryPolicy.close();
		m_writePolicy.close();
	}
}
