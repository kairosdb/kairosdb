package org.kairosdb.core.datastore.queue;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;

/**
 Created by bhawkins on 1/7/16.
 */
public class KryoDataPoint
{
	private String m_metricName;
	private ImmutableSortedMap<String, String> m_tags;
	private DataPoint m_dataPoint;
	private int m_ttl;

}
