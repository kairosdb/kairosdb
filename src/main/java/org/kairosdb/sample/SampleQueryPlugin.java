package org.kairosdb.sample;

import org.kairosdb.core.annotation.PluginName;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryPlugin;
import org.kairosdb.datastore.cassandra.BatchHandler;
import org.kairosdb.datastore.cassandra.CassandraRowKeyPlugin;
import org.kairosdb.datastore.cassandra.DataPointsRowKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;


/**
 This sample code shows what a plugin would look like that implements
 CassandraRowKeyPlugin.

 Here is a sample curl call that will use this plugin once it is bound into
 the application.
 curl -v -w "\n"  --url 'http://localhost:8080/api/v1/datapoints/query' --header 'accept: application/json' --header 'content-type: application/json' --data '{"start_relative":{"value":"1","unit":"days"},"metrics":[{"name":"dummyStatus", "plugins": [{"name": "samplePlugin", "message": "Hello World"}] }] }'
 */
@PluginName(name = "samplePlugin", description = "")
public class SampleQueryPlugin implements QueryPlugin, CassandraRowKeyPlugin
{
	public static final Logger logger = LoggerFactory.getLogger(BatchHandler.class);

	@Override
	public Iterator<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query)
	{
		logger.info("getKeysForQueryIterator was called");
		return new ArrayList().iterator();
	}

	public void setMessage(String message)
	{
		logger.info("And the message is {}", message);
	}

	@Override
	public String getName()
	{
		return "samplePlugin";
	}
}
