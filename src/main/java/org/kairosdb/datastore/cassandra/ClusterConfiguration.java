package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import org.kairosdb.core.KairosConfig;
import org.kairosdb.core.datastore.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static org.kairosdb.datastore.cassandra.RowSpec.DEFAULT_ROW_WIDTH;

public class ClusterConfiguration
{
	public static final Logger logger = LoggerFactory.getLogger(ClusterConnection.class);
	private final Splitter PortSplitter = Splitter.on(':')
			.trimResults().omitEmptyStrings();

	private final String m_keyspace;
	private final ConsistencyLevel m_readConsistencyLevel;
	private final ConsistencyLevel m_writeConsistencyLevel;
	private final int m_requestRetryCount;
	private final Map<String, Integer> m_hostList;
	private final String m_clusterName;
	private final Multimap<String, String> m_tagIndexedMetrics;
	private final Config m_config;
	private final String m_replication;
	private final long m_startTime;
	private final long m_endTime;
	private final long m_rowWidth;
	private final TimeUnit m_rowUnit;

	public ClusterConfiguration(KairosConfig config) throws ParseException
	{
		//todo load defaults into a config before getting values using withfallback

		m_config = config.getRawConfig();
		m_keyspace = config.getString("keyspace", "kairosdb");
		m_replication = config.getString("replication", "{'class': 'SimpleStrategy','replication_factor' : 1}");
		m_clusterName = config.getString("name", "default");
		m_readConsistencyLevel = DefaultConsistencyLevel.valueOf(config.getString("read_consistency_level", "ONE"));
		m_writeConsistencyLevel = DefaultConsistencyLevel.valueOf(config.getString("write_consistency_level", "QUORUM"));

		m_rowUnit = TimeUnit.valueOf(config.getString("row_time_unit", TimeUnit.MILLISECONDS.toString()));

		if (!(m_rowUnit == TimeUnit.SECONDS || m_rowUnit == TimeUnit.MILLISECONDS))
			throw new ParseException("The row_time_unit for a cluster must be either SECONDS or MILLISECONDS", -1);

		long defaultWidth = DEFAULT_ROW_WIDTH;
		if (m_rowUnit == TimeUnit.SECONDS)
			defaultWidth = defaultWidth / 1000;

		m_rowWidth = config.getLong("row_width", defaultWidth);

		m_requestRetryCount = config.getInt("request_retry_count", 2);

		List<String> hostList = config.getStringList("cql_host_list", Collections.singletonList("localhost"));

		ImmutableMap.Builder<String, Integer> hostBuilder = ImmutableMap.<String, Integer>builder();
		for (String hostEntry : hostList)
		{
			Iterator<String> hostPort = PortSplitter.split(hostEntry).iterator();

			String host = hostPort.next();
			int port = 9042;

			if (hostPort.hasNext())
				port = Integer.parseInt(hostPort.next());

			hostBuilder.put(host, port);
		}

		m_hostList = hostBuilder.build();

		Date startDate = config.getDateTime("start_time");
		if (startDate != null)
			m_startTime = startDate.getTime();
		else
			m_startTime = Long.MIN_VALUE;

		Date endDate = config.getDateTime("end_time");
		if (endDate != null)
			m_endTime = endDate.getTime();
		else
			m_endTime = Long.MAX_VALUE;

		checkState(m_startTime < m_endTime, "Cluster start time must be before end time");

		m_tagIndexedMetrics = HashMultimap.create();
		if (config.hasPath("tag_indexed_row_key_lookup_metrics"))
		{
			try
			{
				ConfigObject indexTagConfig = config.getObjectMap("tag_indexed_row_key_lookup_metrics");

				for (Map.Entry<String, ConfigValue> configEntry : indexTagConfig.entrySet())
				{
					Object metricTags = configEntry.getValue().unwrapped();
					if (metricTags instanceof List)
					{
						@SuppressWarnings("unchecked")
						List<String> tagList = (List<String>) metricTags;
						if (tagList.isEmpty())
							m_tagIndexedMetrics.put(configEntry.getKey(), "*");

						for (String metricTag : tagList)
						{
							m_tagIndexedMetrics.put(configEntry.getKey(), metricTag);
						}
					}
					else
					{
						throw new ParseException("tag_indexed_row_key_lookup_metrics must be a list of string or an object, see config documentation.", -1);
					}
				}

			}
			catch (ConfigException.WrongType wt)
			{
				List<String> indexedTags = config.getStringList("tag_indexed_row_key_lookup_metrics");
				for (String indexedTag : indexedTags)
				{
					m_tagIndexedMetrics.put(indexedTag, "*");
				}
			}
		}
	}

	public Config getRawConfig() { return m_config; }

	public String getKeyspace()
	{
		return m_keyspace;
	}

	public ConsistencyLevel getReadConsistencyLevel()
	{
		return m_readConsistencyLevel;
	}

	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return m_writeConsistencyLevel;
	}


	public Map<String, Integer> getHostList()
	{
		return m_hostList;
	}

	public String getClusterName()
	{
		return m_clusterName;
	}

	public String getReplication()
	{
		return m_replication;
	}

	public int getRequestRetryCount()
	{
		return m_requestRetryCount;
	}

	public long getStartTime()
	{
		return m_startTime;
	}

	public long getEndTime()
	{
		return m_endTime;
	}

	public boolean containRange(long queryStartTime, long queryEndTime)
	{
		return (!(queryEndTime < m_startTime || queryStartTime > m_endTime));
	}

	public Multimap<String, String> getTagIndexedMetrics()
	{
		return m_tagIndexedMetrics;
	}

	public TimeUnit getRowTimeUnit()
		{
		return m_rowUnit;
		}

	public long getRowWidth()
		{
		return m_rowWidth;
		}
}

