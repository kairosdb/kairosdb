package org.kairosdb.core.reporting;

import org.kairosdb.metrics4j.collectors.DurationCollector;
import org.kairosdb.metrics4j.collectors.LongCollector;
import org.kairosdb.metrics4j.collectors.StringCollector;


public interface QueryStats
{
	String METRIC_NAME_TAG = "metric_name";
	String QUERY_INDEX_TAG = "query_index";

	//kairosdb.datastore.queries_waiting
	LongCollector queriesWaiting();

	DurationCollector queryStartTime();

	//kairosdb.datastore.query_time
	DurationCollector queryTime();

	//kairosdb.datastore.query_sample_size
	LongCollector querySampleSize();

	//kairosdb.datastore.query_row_count
	LongCollector queryRowCount();

	//kairosdb.datastore.query_collisions
	LongCollector queryCollisions();

	interface Trace
	{
		//kairosdb.log.query.remote_address
		StringCollector remoteAddress();

		//kairosdb.log.query.json
		StringCollector json();
	}
}
