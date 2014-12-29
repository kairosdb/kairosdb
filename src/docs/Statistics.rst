===================
KairosDB Statistics
===================

KairosDB writes internal metrics to the data store so you can monitor the server's performance. Because the metrics are stored in the data store you can use the UI to view the statistics.

These internal metrics are written once per minute by default. You can adjust how often they are reported by modifying the reporter period and report period unit properties in the kairosdb.properties file.

::

   kairosdb.reporter.period=1
   kairosdb.reporter.period_unit=minute

The possible values for kairosdb.reporter.period_unit are milliseconds, seconds, minutes, hours, and days.

If you change these properties you must restart KairosDB for the changes to take effect.

You can turn off metrics reporting by removing the kairosdb.service.reporter property from the property file.

----------------
Metrics Reported
----------------

* *kairosdb.datastore.cassandra.key_query_time* - Time in milliseconds to query the row keys from Cassandra.
* *kairosdb.datastore.query_collisions* - Number of identical queries that are ran at the same time.  Or one was started before the other finished.
* *kairosdb.datastore.query_row_count* - The number of rows a query retrieved data from.
* *kairosdb.datastore.query_sample_size* - The number of data points a query retrieves from Cassandra (before aggregation).
* *kairosdb.datastore.query_time* - The number of milliseconds to retreive the data out of Cassandra for a query (not including key lookup).
* *kairosdb.datastore.write_size* - The number of data points written to the data store during the last write.
* *kairosdb.http.ingest_count* - The number of data points ingested via HTTP since the last report.
* *kairosdb.http.ingest_time* - The amount of time to ingest the number of metrics from kairosdb.http.ingest_count.  So ingest_count / ingest_time is an average of how fast a single metric is inserted.
* *kairosdb.http.query_time* - The amount of time a query takes from processing the request to formating the response.  Does not include time to send data to client.
* *kairosdb.http.request_time* - The total amount of time an HTTP request takes from recieving data to sending response.
* *kairosdb.jvm.free_memory* - The amount of free memory available in the JVM.
* *kairosdb.jvm.total_memory* - The amount of total memory in the JVM.
* *kairosdb.jvm.max_memory* - The maximum amount of memory the JVM will attempt to use.
* *kairosdb.jvm.thread_count* - The total number of threads running in the JVM.
* *kairosdb.metric_counters* - Counts the number of data points received since the last report.  Tags are used to separate one metric from another.
* *kairosdb.protocol.http_request_count* - The number of HTTP requests for each method. This includes a method tag that indicates the method that was called. For example, method=query if a query was done.
* *kairosdb.protocol.telnet_request_count* - The number of telnet requests for each method. This includes a method tag that indicates the method that called. For example, method=put if the put method was called.