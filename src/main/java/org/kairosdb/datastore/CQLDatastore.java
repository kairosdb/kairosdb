package org.kairosdb.datastore;

import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.cassandra.DataCache;
import org.kairosdb.datastore.cassandra.DataPointsRowKey;

import java.util.concurrent.LinkedBlockingDeque;

public class CQLDatastore implements Datastore
{
	private static final long ROW_WIDTH = 1814400000L; //3 Weeks wide
	private static final String KEYSPACE = "kairosdb";

	private final DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(1024);
	private final LinkedBlockingDeque<BoundStatement> queue = new LinkedBlockingDeque<BoundStatement>();
//	private final PreparedStatement datapointStatement;

	private Cluster cluster;
	private Session session;

	// todo remove
	public static void main(String[] args) throws DatastoreException, InterruptedException
	{
		CQLDatastore datastore = new CQLDatastore();

		Cluster cluster = Cluster.builder()
				.addContactPoint("localhost")
				.build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}

		Session session = cluster.connect();

		PreparedStatement statement = session.prepare("INSERT INTO kairosdb.data_points (?) VALUES(?);");
		statement.bind(29929292, 4);


		datastore.close();
	}

	public CQLDatastore()
	{
		connect("localhost");
//		datapointStatement = session.prepare("INSERT INTO " + KEYSPACE + ".data_points (..., ...) VALUES (?,?,?);");
	}

	public void connect(String node) {
		cluster = Cluster.builder()
				.addContactPoint(node)
				.build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}

		session = cluster.connect();

//		ResultSet resultSet = session.execute("Select * from kairosdb.string_index");
//		for (Row row : resultSet)
//		{
//			System.out.println(row);
//		}
	}

	@Override
	public void close() throws InterruptedException, DatastoreException
	{
		cluster.close();
	}

	@Override
	public void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint) throws DatastoreException
	{
		try
		{
			long rowTime = -1L;
			DataPointsRowKey rowKey = null;
			//time the data is written.
			long writeTime = System.currentTimeMillis();

			if (dataPoint.getTimestamp() < 0)
				throw new DatastoreException("Timestamp must be greater than or equal to zero.");
			long newRowTime = calculateRowTime(dataPoint.getTimestamp());
			if (newRowTime != rowTime)
			{
				rowTime = newRowTime;
				rowKey = new DataPointsRowKey(metricName, rowTime, dataPoint.getDataStoreDataType(),
						tags);

				long now = System.currentTimeMillis();
//				//Write out the row key if it is not cached
//				if (!m_rowKeyCache.isCached(rowKey))
//					m_rowKeyWriteBuffer.addData(metricName, rowKey, "", now);
//
//				//Write metric name if not in cache
//				if (!m_metricNameCache.isCached(metricName))
//				{
//					if (metricName.length() == 0)
//					{
//						logger.warn(
//								"Attempted to add empty metric name to string index. Row looks like: "+dataPoint
//						);
//					}
//					m_stringIndexWriteBuffer.addData(ROW_KEY_METRIC_NAMES,
//							metricName, "", now);
//				}
//
//				//Check tag names and values to write them out
//				for (String tagName : tags.keySet())
//				{
//					if (!m_tagNameCache.isCached(tagName))
//					{
//						if(tagName.length() == 0)
//						{
//							logger.warn(
//									"Attempted to add empty tagName to string cache for metric: "+metricName
//							);
//						}
//						m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_NAMES,
//								tagName, "", now);
//
//					}
//
//					String value = tags.get(tagName);
//					if (!m_tagValueCache.isCached(value))
//					{
//						if(value.toString().length() == 0)
//						{
//							logger.warn(
//									"Attempted to add empty tagValue (tag name "+tagName+") to string cache for metric: "+metricName
//							);
//						}
//						m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_VALUES,
//								value, "", now);
//					}
//				}
			}

			int columnName = getColumnName(rowTime, dataPoint.getTimestamp());
			session.execute("INSERT INTO " + KEYSPACE + ".data_points (" + columnName + ") VALUES(" + dataPoint.getLongValue() + ");");
			// todo check result
		}
		catch (DatastoreException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new DatastoreException(e);
		}
	}

	@Override
	public Iterable<String> getMetricNames() throws DatastoreException
	{
		return null;
	}

	@Override
	public Iterable<String> getTagNames() throws DatastoreException
	{
		return null;
	}

	@Override
	public Iterable<String> getTagValues() throws DatastoreException
	{
		return null;
	}

	@Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
	{

	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
	{

	}

	@Override
	public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
	{
		return null;
	}

	public static long calculateRowTime(long timestamp)
	{
		return (timestamp - (timestamp % ROW_WIDTH));
	}

	@SuppressWarnings("PointlessBitwiseExpression")
	public static int getColumnName(long rowTime, long timestamp)
	{
		int ret = (int) (timestamp - rowTime);

		/*
			The timestamp is shifted to support legacy datapoints that
			used the extra bit to determine if the value was long or double
		 */
		return (ret << 1);
	}
}
