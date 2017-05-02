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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mchange.v2.c3p0.DataSources;
import org.agileclick.genorm.runtime.GenOrmQueryResultSet;
import org.h2.jdbcx.JdbcDataSource;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.*;
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
import org.kairosdb.datastore.h2.orm.MetricNamesQuery;
import org.kairosdb.datastore.h2.orm.MetricTag;
import org.kairosdb.datastore.h2.orm.ServiceIndex;
import org.kairosdb.datastore.h2.orm.ServiceIndex_base;
import org.kairosdb.datastore.h2.orm.Tag;
import org.kairosdb.datastore.h2.orm.TagNamesQuery;
import org.kairosdb.datastore.h2.orm.TagValuesQuery;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class H2Datastore implements Datastore, ServiceKeyStore
{
	public static final Logger logger = LoggerFactory.getLogger(H2Datastore.class);
	public static final String DATABASE_PATH_PROPERTY = "kairosdb.datastore.h2.database_path";

	private Connection m_holdConnection;  //Connection that holds the database open
	private final KairosDataPointFactory m_dataPointFactory;
	private final EventBus m_eventBus;

	@Inject
	public H2Datastore(@Named(DATABASE_PATH_PROPERTY) String dbPath, 
			KairosDataPointFactory dataPointFactory,
			EventBus eventBus) throws DatastoreException
	{
		m_dataPointFactory = dataPointFactory;
		m_eventBus = eventBus;
		boolean createDB = false;

		File dataDir = new File(dbPath);
		if (!dataDir.exists())
			createDB = true;
	
		dbPath = dbPath.replace('\\', '/');
		//newer H2 is more strict about relative paths
		String jdbcPath = (dataDir.isAbsolute() || dbPath.startsWith("./") ? "" : "./") + dbPath;
		logger.info("Starting H2 database in " + jdbcPath);
		
		JdbcDataSource ds = new JdbcDataSource();
		ds.setURL("jdbc:h2:" + jdbcPath + "/kairosdb");
		ds.setUser("sa");

		try
		{
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

	@Override
	public void close()
	{
		try
		{
			if (m_holdConnection != null)
				m_holdConnection.close();
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
				DataPointsRowKey dataPointsRowKey = new DataPointsRowKey(metricName,
						0, dataPoint.getDataStoreDataType(), tags);
				m_eventBus.post(new RowKeyEvent(metricName, dataPointsRowKey, 0));

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
	public Iterable<String> getMetricNames()
	{
		MetricNamesQuery query = new MetricNamesQuery();
		MetricNamesQuery.ResultSet results = query.runQuery();

		List<String> metricNames = new ArrayList<>();
		while (results.next())
		{
			metricNames.add(results.getRecord().getName());
		}

		results.close();

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

		//Manually build the where clause for the tags
		//This is subject to sql injection
		Set<String> filterTags = query.getTags().keySet();
		if (filterTags.size() != 0)
		{
			sb.append(" and (");
			boolean first = true;
			for (String tag : filterTags)
			{
				if (!first)
					sb.append(" or ");
				first = false;

				sb.append(" (mt.\"tag_name\" = '").append(tag);
				sb.append("' and (");

				Set<String> values = query.getTags().get(tag);
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

			idQuery = new MetricIdsWithTagsQuery(query.getName(), filterTags.size(),
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
				Map<String, String> tagMap = new TreeMap<>();

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
					boolean startedDataPointSet = false;
					while (resultSet.next())
					{
						if (!startedDataPointSet)
						{
							queryCallback.startDataPointSet(type, tagMap);
							startedDataPointSet = true;
						}

						DataPoint record = resultSet.getRecord();

						queryCallback.addDataPoint(m_dataPointFactory.createDataPoint(type,
								record.getTimestamp().getTime(),
								KDataInput.createInput(record.getValue())));
					}
				}
				finally
				{
					resultSet.close();
				}
			}
			queryCallback.endDataPoints();
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
	public void setValue(String service, String serviceKey, String key, String value) throws DatastoreException
	{
		GenOrmDataSource.attachAndBegin();
		try
		{
			ServiceIndex serviceIndex = ServiceIndex.factory.findOrCreate(service, serviceKey, key);
			if (value != null)
				serviceIndex.setValue(value);

			GenOrmDataSource.commit();
		}
		finally
		{
			GenOrmDataSource.close();
		}
	}

	@Override
	public String getValue(String service, String serviceKey, String key) throws DatastoreException
	{
		ServiceIndex serviceIndex = ServiceIndex.factory.find(service, serviceKey, key);
		if (serviceIndex != null)
			return serviceIndex.getValue();
		else
			return null;
	}

	@Override
	public Iterable<String> listServiceKeys(String service) throws DatastoreException
	{
		final ServiceIndex_base.ResultSet keys = ServiceIndex.factory.getServiceKeys(service);

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
						return keys.next();
					}

					@Override
					public String next()
					{
						return keys.getRecord().getServiceKey();
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
		final ServiceIndex_base.ResultSet keys = ServiceIndex.factory.getKeys(service, serviceKey);

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
						return keys.next();
					}

					@Override
					public String next()
					{
						return keys.getRecord().getKey();
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
		final ServiceIndex_base.ResultSet keys = ServiceIndex.factory.getKeysLike(service, serviceKey, keyStartsWith+"%");

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
						return keys.next();
					}

					@Override
					public String next()
					{
						return keys.getRecord().getKey();
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
            GenOrmDataSource.commit();
        }
        finally
        {
            GenOrmDataSource.close();
        }
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
