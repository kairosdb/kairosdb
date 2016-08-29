package org.kairosdb.datastore.remote;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;

/**
 Created by bhawkins on 8/29/16.
 */
public class RemoteListener implements DataPointListener
{
	public static final Logger logger = LoggerFactory.getLogger(RemoteListener.class);
	private final RemoteDatastore m_remoteDatastore;

	@Inject
	public RemoteListener(RemoteDatastore remoteDatastore)
	{
		m_remoteDatastore = remoteDatastore;
	}

	@Override
	public void dataPoint(String metricName, SortedMap<String, String> tags, DataPoint dataPoint)
	{
		try
		{
			m_remoteDatastore.putDataPoint(metricName,
					ImmutableSortedMap.copyOfSorted(tags), dataPoint, 0);
		}
		catch (DatastoreException e)
		{
			logger.error("Error writing to remote datastore", e);
		}
	}
}
