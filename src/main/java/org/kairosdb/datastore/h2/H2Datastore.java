/*
 * Copyright 2013 Proofpoint Inc.
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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mchange.v2.c3p0.DataSources;
import org.agileclick.genorm.runtime.GenOrmQueryResultSet;
import org.h2.jdbcx.JdbcDataSource;
import org.kairosdb.core.*;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.h2.orm.*;
import org.kairosdb.datastore.h2.orm.DataPoint;
import org.kairosdb.util.KDataInput;
import org.kairosdb.util.KDataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;

public class H2Datastore implements Datastore
{
	public static final Logger logger = LoggerFactory.getLogger(H2Datastore.class);
	public static final String DATABASE_PATH_PROPERTY = "kairosdb.datastore.h2.database_path";

	private Connection m_holdConnection;  //Connection that holds the database open
	private KairosDataPointFactory m_dataPointFactory;

	@Inject
	public H2Datastore(@Named(DATABASE_PATH_PROPERTY) String dbPath, 
			KairosDataPointFactory dataPointFactory) throws DatastoreException
	{
		m_dataPointFactory = dataPointFactory;
		logger.info("Starting H2 database in " + dbPath);
		boolean createDB = false;

		File dataDir = new File(dbPath);
		if (!dataDir.exists())
			createDB = true;

		JdbcDataSource ds = new JdbcDataSource();
		ds.setURL("jdbc:h2:" + dbPath + "/kairosdb");
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
		InputStreamReader reader = new InputStreamReader(getClass().getClassLoader()
				.getResourceAsStream("create.sql"));

		int ch;
		while ((ch = reader.read()) != -1)
			sb.append((char) ch);

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

	@Override
	public synchronized void putDataPoint(String metricName,
			ImmutableSortedMap<String, String> tags,
			org.kairosdb.core.DataPoint dataPoint, int ttl) throws DatastoreException
	{
		GenOrmDataSource.attachAndBegin();
		try
		{
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

		List<String> metricNames = new ArrayList<String>();
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

		List<String> tagNames = new ArrayList<String>();
		while (results.next())
			tagNames.add(results.getRecord().getName());

		results.close();

		return (tagNames);
	}

	@Override
	public Iterable<String> getTagValues()
	{
		TagValuesQuery.ResultSet results = new TagValuesQuery().runQuery();

		List<String> tagValues = new ArrayList<String>();
		while (results.next())
			tagValues.add(results.getRecord().getValue());

		results.close();

		return (tagValues);
	}

	private GenOrmQueryResultSet<? extends MetricIdResults> getMetricIdsForQuery(DatastoreMetricQuery query)
	{
		StringBuilder sb = new StringBuilder();

		GenOrmQueryResultSet<? extends MetricIdResults> idQuery = null;

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
				Map<String, String> tagMap = new TreeMap<String, String>();

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
