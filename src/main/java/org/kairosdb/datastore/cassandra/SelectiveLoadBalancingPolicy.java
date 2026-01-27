package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.*;


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
	public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter)
	{
		m_queryPolicy.init(nodes, distanceReporter);
		m_writePolicy.init(nodes, distanceReporter);
	}

	/*public HostDistance distance(Node host)
	{
		return m_writePolicy.distance(host);
	}*/

	@NonNull
	@Override
	public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session)
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
	public void onAdd(Node host)
	{
		m_queryPolicy.onAdd(host);
		m_writePolicy.onAdd(host);
	}

	@Override
	public void onUp(Node host)
	{
		m_queryPolicy.onUp(host);
		m_writePolicy.onUp(host);
	}

	@Override
	public void onDown(Node host)
	{
		m_queryPolicy.onDown(host);
		m_writePolicy.onDown(host);
	}

	@Override
	public void onRemove(Node host)
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
