package org.kairosdb.core.http.rest;

import org.kairosdb.metrics4j.annotation.Help;
import org.kairosdb.metrics4j.collectors.DurationCollector;
import org.kairosdb.metrics4j.collectors.LongCollector;

import java.time.Duration;

public interface HttpStats
{
	//kairosdb.http.request_time
	@Help("Time for doing an a query but counts all queries in a single request")
	DurationCollector requestTime();

	//kairosdb.http.query_time
	@Help("Just the amount of time to query a single metric")
	DurationCollector queryTime();

	//kairosdb.http.ingest_count
	@Help("Datapoints received via the http protocol")
	LongCollector ingestCount();

	//kariosdb.http.ingest_time
	@Help("Amount of time to ingest data using an http request")
	DurationCollector ingestTime();
}
