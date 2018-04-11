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
* *kairosdb.datastore.cassandra.row_key_count* - The number of partitions in Cassandra the query came from.
* *kairosdb.datastore.cassandra.client.blocking_executor_queue_depth* - The number of queued up tasks in the non-blocking executor.
* *kairosdb.datastore.cassandra.client.connected_to_hosts* - The number of Cassandra hosts is currently connected.
* *kairosdb.datastore.cassandra.client.executor_queue_depth* - The number of queued up tasks in the non-blocking executor.
* *kairosdb.datastore.cassandra.client.known_hosts* - The number of Cassandra hosts currently known (that is whether they are currently considered up or down).
* *kairosdb.datastore.cassandra.client.open_connections* - The total number of currently opened connections to Cassandra hosts.
* *kairosdb.datastore.cassandra.client.reconnection_scheduler_queue_size* - The size of the work queue for the reconnection scheduler (Reconnection).
* *kairosdb.datastore.cassandra.client.requests_timer.avg* -  The average number of user requests performed on the Cassandra cluster.
* *kairosdb.datastore.cassandra.client.requests_timer.count* - The total number of user requests performed on the Cassandra cluster.
* *kairosdb.datastore.cassandra.client.requests_timer.max* - The maximum number of user requests performed on the Cassandra cluster.
* *kairosdb.datastore.cassandra.client.requests_timer.min* - The minimum number of user requests performed on the Cassandra cluster.
* *kairosdb.datastore.cassandra.client.task_scheduler_queue_size* - The size of the work queue for the task scheduler (Scheduled Tasks).
* *kairosdb.datastore.cassandra.client.trashed_connections* - The total number of currently "trashed" connections to Cassandra hosts.
* *kairosdb.datastore.cassandra.write_batch_size.avg* - The average size of the batch of data written to Casandra.
* *kairosdb.datastore.cassandra.write_batch_size.count* - The total number of items in a batch of data written to Cassandra.
* *kairosdb.datastore.cassandra.write_batch_size.max* - The maximum size of a batch of data written to Cassandra.
* *kairosdb.datastore.cassandra.write_batch_size.min* - The minimum size of a batch of data written to Cassandra.
* *kairosdb.datastore.cassandra.write_batch_size.sum* - The total size of a batch of data written to Cassandra.
* *kairosdb.datastore.query_collisions* - Number of identical queries that are ran at the same time.  Or one was started before the other finished.
* *kairosdb.datastore.query_row_count* - The number of chunks fetched by the datastore.  Example. If it takes two queries to get the data from a single row in Cassandra then this value will be 2.  The higher this number the more memory the query will require to process.
* *kairosdb.datastore.query_sample_size* - The number of data points a query retrieves from Cassandra (before aggregation).
* *kairosdb.datastore.query_time* - The number of milliseconds to retreive the data out of Cassandra for a query.
* *kairosdb.datastore.write_size* - The number of data points written to the data store during the last write.
* *kairosdb.http.ingest_count* - The number of data points ingested via HTTP since the last report.
* *kairosdb.http.ingest_time* - The amount of time to ingest the number of metrics from kairosdb.http.ingest_count. More specifically, it is the time it takes for KairosDB to process the incoming data points and add them to a queue to be flushed to Cassandra. Ingest_count / ingest_time is an average of how fast a single metric is inserted.
* *kairosdb.http.query_time* - The amount of time a query takes from processing the request to formating the response.  Does not include time to send data to client.
* *kairosdb.http.request_time* - The total amount of time an HTTP request takes from recieving data to sending response.
* *kairosdb.ingest_executor.write_time_micro.avg* - The average time datapoints are ingested (in microseconds).
* *kairosdb.ingest_executor.write_time_micro.count* - The number of items ingested.
* *kairosdb.ingest_executor.write_time_micro.max* - The maximum time datapoints are ingested (in microseconds).
* *kairosdb.ingest_executor.write_time_micro.min* - The minimum time datapoints are ingested (in microseconds).
* *kairosdb.ingest_executor.write_time_micro.sum* - The total time datatponts are ingested (in microseconds).
* *kairosdb.jvm.free_memory* - The amount of free memory available in the JVM.
* *kairosdb.jvm.max_memory* - The maximum amount of memory the JVM will attempt to use.
* *kairosdb.jvm.thread_count* - The total number of threads running in the JVM.
* *kairosdb.jvm.total_memory* - The amount of total memory in the JVM.
* *kairosdb.log.query.json* - When enabled by turning on kairosdb.log.queries.enable this metric contains the json query.
* *kairosdb.log.query.remote_address* - When enabled by turning on kairosdb.log.queries.enable this metric contains the sender IP address for the query.
* *kairosdb.metric_counters* - Counts the number of data points received since the last report.  Tags are used to separate one metric from another.
* *kairosdb.protocol.http_request_count* - The number of HTTP requests for each method. This includes a method tag that indicates the method that was called. For example, method=query if a query was done.
* *kairosdb.protocol.telnet_request_count* - The number of telnet requests for each method. This includes a method tag that indicates the method that called. For example, method=put if the put method was called.
* *kairosdb.queue.batch_stats.count* - The number of batches.
* *kairosdb.queue.batch_stats.max* - The maximum size of data points in a queued batch.
* *kairosdb.queue.batch_stats.min* - The minimum size of data points in a queued batch.
* *kairosdb.queue.batch_stats.sum* - The number of data points in batches since last reported.
* *kairosdb.queue.file_queue.size* - Number of data points in the file queue.
* *kairosdb.queue.process_count* - The number of data points read from the queue since last reported.
* *kairosdb.queue.read_from_file* - The number of data points read from the file.