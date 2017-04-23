package org.kairosdb.datastore.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.EventBus;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.queue.EventCompletionCallBack;
import org.kairosdb.events.BatchReductionEvent;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.events.RowKeyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.kairosdb.datastore.cassandra.CassandraDatastore.ROW_WIDTH;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.calculateRowTime;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.getColumnName;

/**
 Created by bhawkins on 1/11/17.
 */
public class BatchHandler implements Callable<Boolean>
{
	public static final Logger logger = LoggerFactory.getLogger(BatchHandler.class);
	public static final Logger failedLogger = LoggerFactory.getLogger("failed_logger");

	private final List<DataPointEvent> m_events;
	private final EventCompletionCallBack m_callBack;
	private final int m_defaultTtl;
	private final DataCache<DataPointsRowKey> m_rowKeyCache;
	private final DataCache<String> m_metricNameCache;
	private final EventBus m_eventBus;
	private final boolean m_fullBatch;
	private final ConsistencyLevel m_consistencyLevel;
	private final Session m_session;
	private final CassandraDatastore.PreparedStatements m_preparedStatements;
	private final BatchStats m_batchStats;
	private final LoadBalancingPolicy m_loadBalancingPolicy;

	public BatchHandler(List<DataPointEvent> events, EventCompletionCallBack callBack,
			int defaultTtl, ConsistencyLevel consistencyLevel, DataCache<DataPointsRowKey>
			rowKeyCache, DataCache<String> metricNameCache, EventBus eventBus,
			Session session, CassandraDatastore.PreparedStatements preparedStatements,
			boolean fullBatch, BatchStats batchStats, LoadBalancingPolicy loadBalancingPolicy)
	{
		m_consistencyLevel = consistencyLevel;
		m_session = session;
		m_preparedStatements = preparedStatements;
		m_batchStats = batchStats;
		m_loadBalancingPolicy = loadBalancingPolicy;

		m_events = events;
		m_callBack = callBack;
		m_defaultTtl = defaultTtl;
		m_rowKeyCache = rowKeyCache;
		m_metricNameCache = metricNameCache;
		m_eventBus = eventBus;
		m_fullBatch = fullBatch;
	}

	public boolean isFullBatch()
	{
		return m_fullBatch;
	}

	private void loadBatch(int limit, CQLBatch batch, Iterator<DataPointEvent> events) throws Exception
	{
		int count = 0;
		while (events.hasNext() && count < limit)
		{
			DataPointEvent event = events.next();
			count++;

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
				batch.addRowKey(metricName, rowKey, rowKeyTtl);

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
				batch.addMetricName(metricName);
			}


			int columnTime = getColumnName(rowTime, dataPoint.getTimestamp());

			batch.addDataPoint(rowKey, columnTime, dataPoint, ttl);
		}
	}


	@Override
	public Boolean call() throws Exception
	{
		int divisor = 1;
		boolean retry = false;
		int limit = Integer.MAX_VALUE;

		do
		{
			retry = false;

			//Used to reduce batch size with each retry
			limit = m_events.size() / divisor;
			try
			{
				Iterator<DataPointEvent> events = m_events.iterator();

				while (events.hasNext())
				{
					CQLBatch batch = new CQLBatch(m_consistencyLevel, m_session, m_preparedStatements,
							m_batchStats, m_loadBalancingPolicy);

					loadBatch(limit, batch, events);

					batch.submitBatch();

				}

			}
			//If More exceptions are added to retry they need to be added to AdaptiveExecutorService
			catch (NoHostAvailableException nae)
			{
				//Throw this out so the back off retry can happen
				logger.error(nae.getMessage());
				throw nae;
			}
			catch (UnavailableException ue)
			{
				//Throw this out so the back off retry can happen
				logger.error(ue.getMessage());
				throw ue;
			}
			catch (Exception e)
			{
				if ("Batch too large".equals(e.getMessage()))
					logger.warn("Batch size is too large");
				else
					logger.error("Error sending data points", e);

				if (limit > 10)
				{
					retry = true;
					logger.info("Retrying batch with smaller limit");
				}
				else
				{
					logger.error("Failed to send data points", e);
					for (DataPointEvent event : m_events)
					{
						StringWriter sw = new StringWriter();
						JSONWriter jsonWriter = new JSONWriter(sw);
						jsonWriter.object();
						jsonWriter.key("name").value(event.getMetricName());
						jsonWriter.key("timestamp").value(event.getDataPoint().getTimestamp());
						jsonWriter.key("value");
						event.getDataPoint().writeValueToJson(jsonWriter);

						jsonWriter.key("tags").object();
						ImmutableSortedMap<String, String> tags = event.getTags();
						for (Map.Entry<String, String> entry : tags.entrySet())
						{
							jsonWriter.key(entry.getKey()).value(entry.getValue());
						}
						jsonWriter.endObject();

						jsonWriter.key("ttl").value(event.getTtl());

						jsonWriter.endObject();

						failedLogger.trace(sw.toString());
					}
				}
			}
			divisor++;

		} while (retry);

		if (limit < m_events.size())
		{
			m_eventBus.post(new BatchReductionEvent(limit));
		}

		m_callBack.complete();

		return m_fullBatch;
	}
}
