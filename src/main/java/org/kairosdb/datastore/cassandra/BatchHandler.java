package org.kairosdb.datastore.cassandra;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.EventBus;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.events.RowKeyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_WIDTH;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.calculateRowTime;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.getColumnName;

/**
 Created by bhawkins on 1/11/17.
 */
public abstract class BatchHandler implements Callable<Long>
{
	public static final Logger logger = LoggerFactory.getLogger(BatchHandler.class);

	private final List<DataPointEvent> m_events;
	private final EventCompletionCallBack m_callBack;
	private final int m_defaultTtl;
	private final DataCache<DataPointsRowKey> m_rowKeyCache;
	private final DataCache<String> m_metricNameCache;
	private final EventBus m_eventBus;

	public BatchHandler(List<DataPointEvent> events, EventCompletionCallBack callBack,
			int defaultTtl, DataCache<DataPointsRowKey> rowKeyCache,
			DataCache<String> metricNameCache, EventBus eventBus)
	{
		m_events = events;
		m_callBack = callBack;
		m_defaultTtl = defaultTtl;
		m_rowKeyCache = rowKeyCache;
		m_metricNameCache = metricNameCache;
		m_eventBus = eventBus;
	}

	protected abstract void addRowKey(String metricName, DataPointsRowKey rowKey,
			int rowKeyTtl);

	protected abstract void addMetricName(String metricName);

	protected abstract void addDataPoint(DataPointsRowKey rowKey, int columnTime,
			DataPoint dataPoint, int ttl) throws IOException;

	protected abstract void submitBatch();

	@Override
	public Long call() throws Exception
	{
		try
		{
			for (DataPointEvent event : m_events)
			{
				String metricName = event.getMetricName();
				/*if (metricName.startsWith("blast"))
					continue;*/

				ImmutableSortedMap<String, String> tags = event.getTags();
				DataPoint dataPoint = event.getDataPoint();
				int ttl = event.getTtl();

				DataPointsRowKey rowKey = null;
				//time the data is written.
				long writeTime = System.currentTimeMillis();
				if (0 == ttl)
					ttl = m_defaultTtl;

				int rowKeyTtl = 0;
				//Row key will expire 3 weeks after the data in the row expires
				if (ttl != 0)
					rowKeyTtl = ttl + ((int) (ROW_WIDTH / 1000));

				long rowTime = calculateRowTime(dataPoint.getTimestamp());

				rowKey = new DataPointsRowKey(metricName, rowTime, dataPoint.getDataStoreDataType(),
						tags);

				//Write out the row key if it is not cached
				DataPointsRowKey cachedKey = m_rowKeyCache.cacheItem(rowKey);
				if (cachedKey == null)
				{
					addRowKey(metricName, rowKey, rowKeyTtl);

					m_eventBus.post(new RowKeyEvent(metricName, rowKey, rowKeyTtl));
				}
				else
					rowKey = cachedKey;

				//Write metric name if not in cache
				String cachedName = m_metricNameCache.cacheItem(metricName);
				if (cachedName == null)
				{
					if (metricName.length() == 0)
					{
						logger.warn(
								"Attempted to add empty metric name to string index. Row looks like: " + dataPoint
						);
					}
					addMetricName(metricName);
				}


				int columnTime = getColumnName(rowTime, dataPoint.getTimestamp());

				addDataPoint(rowKey, columnTime, dataPoint, ttl);
			}

			submitBatch();

			m_callBack.complete();

		}
		catch (Exception e)
		{
			logger.error("Error sending data points", e);
		}

		return null;
	}
}
