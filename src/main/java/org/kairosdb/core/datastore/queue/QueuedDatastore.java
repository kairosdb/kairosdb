package org.kairosdb.core.datastore.queue;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;
import se.ugli.bigqueue.FanOutQueue;

/**
 QueuedDatastore receives data points and writes them immediately to a file
 backed queue (bigqueue).

 Created by bhawkins on 1/7/16.
 */
public abstract class QueuedDatastore implements Datastore
{
	private Kryo m_kryo;
	private FanOutQueue m_bigQueue;

	public QueuedDatastore(KairosDataPointFactory kairosDataPointFactory)
	{
		m_kryo = new Kryo();
		m_kryo.addDefaultSerializer(DataPoint.class, new KryoDataPointSerializer(kairosDataPointFactory));




	}

	public void putDataPoint(String metricName,
			ImmutableSortedMap<String, String> tags,
			DataPoint dataPoint,
			int ttl) throws DatastoreException
	{

	}

}
