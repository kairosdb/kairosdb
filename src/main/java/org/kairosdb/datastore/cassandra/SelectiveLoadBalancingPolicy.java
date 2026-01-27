package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.cql.BatchStatement;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;

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
	public void init(Map<UUID, Node> nodes, DistanceReporter distanceReporter)
	{
		m_queryPolicy.init(nodes, distanceReporter);
		m_writePolicy.init(nodes, distanceReporter);
	}

	/*public HostDistance distance(Node host)
	{
		return m_writePolicy.distance(host);
	}*/

	@Override
	public Queue<Node> newQueryPlan(Request request, Session session)
	{
		if (request instanceof BatchStatement)
		{
			return m_writePolicy.newQueryPlan(request, session);
		}
		else
		{
			return m_queryPolicy.newQueryPlan(request, session);
		}
	}

	@Override
	public void onAdd(Node node)
	{
		m_queryPolicy.onAdd(node);
		m_writePolicy.onAdd(node);
	}

	@Override
	public void onUp(Node node)
	{
		m_queryPolicy.onUp(node);
		m_writePolicy.onUp(node);
	}

	@Override
	public void onDown(Node node)
	{
		m_queryPolicy.onDown(node);
		m_writePolicy.onDown(node);
	}

	@Override
	public void onRemove(Node node)
	{
		m_queryPolicy.onRemove(node);
		m_writePolicy.onRemove(node);
	}

	@Override
	public void close()
	{
		m_queryPolicy.close();
		m_writePolicy.close();
	}
}
