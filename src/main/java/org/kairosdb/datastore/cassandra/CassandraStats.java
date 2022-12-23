package org.kairosdb.datastore.cassandra;


import org.kairosdb.metrics4j.collectors.DurationCollector;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface CassandraStats
{
	//kairosdb.datastore.cassandra.key_query_time
	DurationCollector keyQueryTime();

	//kairosdb.datastore.cassandra.row_key_count
	LongCollector rowKeyCount();

	//kairosdb.datastore.cassandra.raw_row_key_count
	LongCollector rawRowKeyCount();

}
