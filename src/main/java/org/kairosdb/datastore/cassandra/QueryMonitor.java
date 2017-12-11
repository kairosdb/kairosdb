package org.kairosdb.datastore.cassandra;

import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.json.Query;

import java.util.concurrent.atomic.AtomicLong;

public class QueryMonitor
{
	private volatile boolean m_keepRunning;
	private Exception m_exception;
	private final long m_limit;
	private final AtomicLong m_counter = new AtomicLong();

	public QueryMonitor(long limit)
	{
		m_limit = limit;
		m_keepRunning = true;
	}

	public void incrementCounter()
	{
		if (m_limit != 0 && m_counter.incrementAndGet() > m_limit)
		{
			m_exception = new DatastoreException("Query exceeded limit of "+m_limit+" data points");
			m_keepRunning = false;
		}
	}

	public boolean keepRunning()
	{
		return m_keepRunning;
	}

	public void failQuery(Exception e)
	{
		m_keepRunning = false;
		m_exception = e;
	}

	public Exception getException()
	{
		return m_exception;
	}
}
