/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.datastore.h2;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mchange.v2.c3p0.DataSources;
import org.agileclick.genorm.runtime.GenOrmQueryResultSet;
import org.h2.jdbcx.JdbcDataSource;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.datastore.TagSetImpl;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.cassandra.DataPointsRowKey;
import org.kairosdb.datastore.h2.orm.DSEnvelope;
import org.kairosdb.datastore.h2.orm.DataPoint;
import org.kairosdb.datastore.h2.orm.DeleteMetricsQuery;
import org.kairosdb.datastore.h2.orm.GenOrmDataSource;
import org.kairosdb.datastore.h2.orm.InsertDataPointQuery;
import org.kairosdb.datastore.h2.orm.Metric;
import org.kairosdb.datastore.h2.orm.MetricIdResults;
import org.kairosdb.datastore.h2.orm.MetricIdsQuery;
import org.kairosdb.datastore.h2.orm.MetricIdsWithTagsQuery;
import org.kairosdb.datastore.h2.orm.MetricNamesPrefixQuery;
import org.kairosdb.datastore.h2.orm.MetricNamesQuery;
import org.kairosdb.datastore.h2.orm.MetricTag;
import org.kairosdb.datastore.h2.orm.MetricTagValuesQuery;
import org.kairosdb.datastore.h2.orm.ServiceIndex;
import org.kairosdb.datastore.h2.orm.ServiceModification;
import org.kairosdb.datastore.h2.orm.Tag;
import org.kairosdb.datastore.h2.orm.TagNamesQuery;
import org.kairosdb.datastore.h2.orm.TagValuesQuery;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.events.RowKeyEvent;
import org.kairosdb.util.KDataInput;
import org.kairosdb.util.KDataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.kairosdb.core.KairosConfigProperties.QUERIES_REGEX_PREFIX;

public class H2Datastore implements Datastore, ServiceKeyStore
{
	public static final Logger logger = LoggerFactory.getLogger(H2Datastore.class);
	public static final String DATABASE_PATH_PROPERTY = "kairosdb.datastore.h2.database_path";
	private static final long MIN_TIME_VALUE = Long.MIN_VALUE / 1000;
	private static final long MAX_TIME_VALUE = Long.MAX_VALUE;

	private Connection m_holdConnection;  //Connection that holds the database open
	private final KairosDataPointFactory m_dataPointFactory;
	private final Publisher<RowKeyEvent> m_rowKeyPublisher;
	private final String m_regexPrefix;

	@Inject
	public H2Datastore(@Named(DATABASE_PATH_PROPERTY) String dbPath, 
			KairosDataPointFactory dataPointFactory,
			FilterEventBus eventBus,
			@Named(QUERIES_REGEX_PREFIX) String regexPrefix) throws DatastoreException
	{
		m_dataPointFactory = dataPointFactory;
		m_rowKeyPublisher = eventBus.createPublisher(RowKeyEvent.class);
		boolean createDB = false;
		m_regexPrefix = regexPrefix;

		File dataDir = new File(dbPath);
		if (!dataDir.exists())
			createDB = true;
	
		dbPath = dbPath.replace('\\', '/');
		//newer H2 is more strict about relative paths
		String jdbcPath = (dataDir.isAbsolute() || dbPath.startsWith("./") ? "" : "./") + dbPath;
		logger.info("Starting H2 database in " + jdbcPath);
		
		JdbcDataSource jdbcds = new JdbcDataSource();
		jdbcds.setURL("jdbc:h2:" + jdbcPath + "/kairosdb");
		jdbcds.setUser("sa");

		DataSource ds = jdbcds;

		try
		{

			//Uncomment this line to detect db leaks in H2
			//ds = new LeakDetectorDataSource(ds, 4, 9);
			GenOrmDataSource.setDataSource(new DSEnvelope(DataSources.pooledDataSource(ds)));
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}

		try
		{
			if (createDB)
				createDatabase(ds);
		}
		catch (SQLException e)
		{
			//TODO
			System.out.println("Oh Crap");
			e.printStackTrace();
		}
		catch (IOException e)
		{
			//TODO
			System.out.println("double oh crap");
			e.printStackTrace();
		}
	}

	private void createDatabase(DataSource ds) throws IOException, SQLException
	{
		logger.info("Creating DB");
		m_holdConnection = ds.getConnection();
		m_holdConnection.setAutoCommit(false);

		StringBuilder sb = new StringBuilder();
		try(InputStreamReader reader = new InputStreamReader(getClass().getClassLoader()
				.getResourceAsStream("create.sql")))
		{

			int ch;
			while ((ch = reader.read()) != -1)
				sb.append((char) ch);
		}

		String[] tableCommands = sb.toString().split(";");

		Statement s = m_holdConnection.createStatement();
		for (String command : tableCommands)
			s.execute(command);

		m_holdConnection.commit();
	}

	/*
	 shutdown is only used by unit tests.
	 */
	public void shutdown()
	{
		try {
			m_holdConnection.createStatement().execute("SHUTDOWN");
		}
		catch (SQLException e) {
			logger.error("Failed shutdown:", e);
		}
	}

	@Override
	public void close()
	{
		try
		{
			if (m_holdConnection != null) {
				m_holdConnection.close();
			}
		}
		catch (SQLException e)
		{
			logger.error("Failed closing last connection:", e);
		}
	}

	@Subscribe
	public synchronized void putDataPoint(DataPointEvent event) throws DatastoreException
	{
		GenOrmDataSource.attachAndBegin();
		try
		{
			ImmutableSortedMap<String, String> tags = event.getTags();
			String metricName = event.getMetricName();
			org.kairosdb.core.DataPoint dataPoint = event.getDataPoint();

			String key = createMetricKey(metricName, tags, dataPoint.getDataStoreDataType());
			Metric m = Metric.factory.findOrCreate(key);
			if (m.isNew())
			{
				m.setName(metricName);
				m.setType(dataPoint.getDataStoreDataType());

				for (String name : tags.keySet())
				{
					String value = tags.get(name);
					Tag.factory.findOrCreate(name, value);
					MetricTag.factory.findOrCreate(key, name, value);
				}

				GenOrmDataSource.flush();
				DataPointsRowKey dataPointsRowKey = new DataPointsRowKey(metricName, "H2",
						0, dataPoint.getDataStoreDataType(), tags);
				m_rowKeyPublisher.post(new RowKeyEvent(metricName, dataPointsRowKey, 0));

			}

			KDataOutput dataOutput = new KDataOutput();
			dataPoint.writeValueToBuffer(dataOutput);

			new InsertDataPointQuery(m.getId(), new Timestamp(dataPoint.getTimestamp()),
					dataOutput.getBytes()).runUpdate();

			GenOrmDataSource.commit();
		}
		catch (IOException e)
		{
			throw new DatastoreException(e);
		}
		finally
		{
			GenOrmDataSource.close();
		}
	}


	@Override
	public Iterable<String> getMetricNames(String prefix)
	{
		List<String> metricNames = new ArrayList<>();
		if (prefix == null)
		{
			MetricNamesQuery query = new MetricNamesQuery();
			MetricNamesQuery.ResultSet results = query.runQuery();

			while (results.next())
			{
				metricNames.add(results.getRecord().getName());
			}

			results.close();
		}
		else
		{
			MetricNamesPrefixQuery query = new MetricNamesPrefixQuery(prefix+"%");
			MetricNamesPrefixQuery.ResultSet results = query.runQuery();

			while (results.next())
			{
				metricNames.add(results.getRecord().getName());
			}

			results.close();
		}

		return (metricNames);
	}

	@Override
	public Iterable<String> getTagNames()
	{
		TagNamesQuery.ResultSet results = new TagNamesQuery().runQuery();

		List<String> tagNames = new ArrayList<>();
		while (results.next())
			tagNames.add(results.getRecord().getName());

		results.close();

		return (tagNames);
	}

	@Override
	public Iterable<String> getTagValues()
	{
		TagValuesQuery.ResultSet results = new TagValuesQuery().runQuery();

		List<String> tagValues = new ArrayList<>();
		while (results.next())
			tagValues.add(results.getRecord().getValue());

		results.close();

		return (tagValues);
	}

	private GenOrmQueryResultSet<? extends MetricIdResults> getMetricIdsForQuery(DatastoreMetricQuery query)
	{
		StringBuilder sb = new StringBuilder();

		GenOrmQueryResultSet<? extends MetricIdResults> idQuery;

		HashMultimap<String, String> filterTags = HashMultimap.create();

		//Check if any of the tags are regex
		for (Map.Entry<String, String> tagPair : query.getTags().entries())
		{
			if (m_regexPrefix.length() != 0 && tagPair.getValue().startsWith(m_regexPrefix))
			{
				String regex = tagPair.getValue().substring(m_regexPrefix.length());

				Pattern pattern = Pattern.compile(regex);

				MetricTagValuesQuery.ResultSet resultSet = (new MetricTagValuesQuery(query.getName(), tagPair.getKey())).runQuery();
				while (resultSet.next())
				{
					String tagValue = resultSet.getRecord().getTagValue();

					if (pattern.matcher(tagValue).matches())
						filterTags.put(tagPair.getKey(), tagValue);
				}
				resultSet.close();

			}
			else
			{
				filterTags.put(tagPair.getKey(), tagPair.getValue());
			}
		}

		//todo query tags and values and run the regex over them placing results in to filterTags

		//Manually build the where clause for the tags
		//This is subject to sql injection
		Set<String> filterTagNames = filterTags.keySet();
		if (filterTagNames.size() != 0)
		{
			sb.append(" and (");
			boolean first = true;
			for (String tag : filterTagNames)
			{
				if (!first)
					sb.append(" or ");
				first = false;

				sb.append(" (mt.\"tag_name\" = '").append(tag);
				sb.append("' and (");

				Set<String> values = filterTags.get(tag);
				boolean firstValue = true;
				for (String value : values)
				{
					if (!firstValue)
						sb.append(" or ");
					firstValue = false;

					sb.append("mt.\"tag_value\" = '").append(value);
					sb.append("' ");
				}

				sb.append(")) ");
			}

			sb.append(") ");

			idQuery = new MetricIdsWithTagsQuery(query.getName(), filterTagNames.size(),
					sb.toString()).runQuery();
		}
		else
		{
			idQuery = new MetricIdsQuery(query.getName()).runQuery();
		}

		return (idQuery);
	}

	@Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
	{
		GenOrmQueryResultSet<? extends MetricIdResults> idQuery = getMetricIdsForQuery(query);

		try
		{
			while (idQuery.next())
			{
				MetricIdResults result = idQuery.getRecord();

				String metricId = result.getMetricId();
				String type = result.getType();

				//Collect the tags in the results
				MetricTag.ResultSet tags = MetricTag.factory.getByMetric(metricId);
				SortedMap<String, String> tagMap = new TreeMap<>();

				while (tags.next())
				{
					MetricTag mtag = tags.getRecord();
					tagMap.put(mtag.getTagName(), mtag.getTagValue());
				}
				tags.close();

				Timestamp startTime = new Timestamp(query.getStartTime());
				Timestamp endTime = new Timestamp(query.getEndTime());

				DataPoint.ResultSet resultSet;
				if (query.getLimit() == 0)
				{
					resultSet = DataPoint.factory.getForMetricId(metricId,
							startTime, endTime, query.getOrder().getText());
				}
				else
				{
					resultSet = DataPoint.factory.getForMetricIdWithLimit(metricId,
							startTime, endTime, query.getLimit(), query.getOrder().getText());
				}

				try
				{
					if (resultSet.next())
					{
						try (QueryCallback.DataPointWriter dataPointWriter =
								     queryCallback.startDataPointSet(type, tagMap))
						{
							do
							{
								DataPoint record = resultSet.getRecord();

								dataPointWriter.addDataPoint(m_dataPointFactory.createDataPoint(type,
										record.getTimestamp().getTime(),
										KDataInput.createInput(record.getValue())));
							} while (resultSet.next());
						}
					}
				}
				finally
				{
					resultSet.close();
				}
			}
		}
		catch (IOException e)
		{
			throw new DatastoreException(e);
		}
		finally
		{
			idQuery.close();
		}
	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{
		GenOrmDataSource.attachAndBegin();
		try
		{
			GenOrmQueryResultSet<? extends MetricIdResults> idQuery =
					getMetricIdsForQuery(deleteQuery);

			while (idQuery.next())
			{
				String metricId = idQuery.getRecord().getMetricId();

				new DeleteMetricsQuery(metricId,
						new Timestamp(deleteQuery.getStartTime()),
						new Timestamp(deleteQuery.getEndTime())).runUpdate();

				if (DataPoint.factory.getWithMetricId(metricId) == null)
				{
					Metric.factory.find(metricId).delete();
				}
			}

			idQuery.close();

			GenOrmDataSource.commit();
		}
		finally
		{
			GenOrmDataSource.close();
		}
	}

	@Override
	public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
	{
		GenOrmQueryResultSet<? extends MetricIdResults> idQuery = getMetricIdsForQuery(query);

		TagSetImpl tagSet = new TagSetImpl();
		try
		{
			while (idQuery.next())
			{
				String metricId = idQuery.getRecord().getMetricId();

				//Collect the tags in the results
				MetricTag.ResultSet tags = MetricTag.factory.getByMetric(metricId);

				while (tags.next())
				{
					MetricTag mtag = tags.getRecord();
					tagSet.addTag(mtag.getTagName(), mtag.getTagValue());
				}

				tags.close();
			}
		}
		finally
		{
			idQuery.close();
		}

		return tagSet;
	}

	@Override
	public void indexMetricTags(DatastoreMetricQuery query) throws DatastoreException
	{
		// H2 does not have an index
	}

	@Override
	public long getMinTimeValue()
	{
		return MIN_TIME_VALUE;
	}

	@Override
	public long getMaxTimeValue()
	{
		return MAX_TIME_VALUE;
	}

	@Override
	public void setValue(String service, String serviceKey, String key, String value) throws DatastoreException
	{
		GenOrmDataSource.attachAndBegin();
		try
		{
			ServiceIndex serviceIndex = ServiceIndex.factory.findOrCreate(service, serviceKey, key);
			if (value != null) {
				serviceIndex.setValue(value);
				long now = System.currentTimeMillis();
				//Need to make sure the modification time always gets updated
				serviceIndex.setModificationTime(new java.sql.Timestamp(now));

				// Update the service key timestamp
				ServiceModification orCreate = ServiceModification.factory.findOrCreate(service, serviceKey);
				orCreate.setModificationTime(new java.sql.Timestamp(now));
			}

			GenOrmDataSource.commit();
		}
		finally
		{
			GenOrmDataSource.close();
		}
	}

	@Override
	public ServiceKeyValue getValue(String service, String serviceKey, String key) throws DatastoreException
	{
		ServiceIndex serviceIndex = ServiceIndex.factory.find(service, serviceKey, key);
		if (serviceIndex != null)
			return new ServiceKeyValue(serviceIndex.getValue(), serviceIndex.getModificationTime());
		else
			return null;
	}

	@Override
	public Iterable<String> listServiceKeys(String service) throws DatastoreException
	{
		final Iterator<ServiceIndex> keys = ServiceIndex.factory.getServiceKeys(service).getArrayList().iterator();

		return new Iterable<String>()
		{
			@Override
			public Iterator<String> iterator()
			{
				return new Iterator<String>()
				{
					@Override
					public boolean hasNext()
					{
						return keys.hasNext();
					}

					@Override
					public String next()
					{
						return keys.next().getServiceKey();
					}

					@Override
					public void remove() { }
				};
			}
		};
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException
	{
		final Iterator<ServiceIndex> keys = ServiceIndex.factory.getKeys(service, serviceKey).getArrayList().iterator();

		return new Iterable<String>()
		{
			@Override
			public Iterator<String> iterator()
			{
				return new Iterator<String>()
				{
					@Override
					public boolean hasNext()
					{
						return keys.hasNext();
					}

					@Override
					public String next()
					{
						return keys.next().getKey();
					}

					@Override
					public void remove() { }
				};
			}
		};
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException
	{
		final Iterator<ServiceIndex> results = ServiceIndex.factory.getKeysLike(service, serviceKey, keyStartsWith+"%").getArrayList().iterator();


		return new Iterable<String>()
		{
			@Override
			public Iterator<String> iterator()
			{
				return new Iterator<String>()
				{
					@Override
					public boolean hasNext()
					{
						return results.hasNext();
					}

					@Override
					public String next()
					{
						return results.next().getKey();
					}

					@Override
					public void remove() { }
				};
			}
		};
	}

    @Override
    public void deleteKey(String service, String serviceKey, String key)
            throws DatastoreException
    {
        GenOrmDataSource.attachAndBegin();
        try
        {
            ServiceIndex.factory.delete(service, serviceKey, key);

            // Update the service key timestamp
			ServiceModification orCreate = ServiceModification.factory.findOrCreate(service, serviceKey);
			orCreate.setModificationTime(new java.sql.Timestamp(System.currentTimeMillis()));

			GenOrmDataSource.commit();
        }
        finally
        {
            GenOrmDataSource.close();
        }
    }

	@Override
	public Date getServiceKeyLastModifiedTime(String service, String serviceKey)
			throws DatastoreException
	{
		ServiceModification serviceModification = ServiceModification.factory.find(service, serviceKey);
		if (serviceModification != null)
			return serviceModification.getModificationTime();
		else
			return null;
	}

	private String createMetricKey(String metricName, SortedMap<String, String> tags,
			String type)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(metricName).append(":");

		sb.append(type).append(":");

		for (String name : tags.keySet())
		{
			sb.append(name).append("=");
			sb.append(tags.get(name)).append(":");
		}

		return (sb.toString());
	}
}
