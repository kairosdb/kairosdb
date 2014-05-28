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

* *kairosdb.datastore.key_write_size* - The number of rows written to the data store during the last write.
* *kairosdb.datastore.write_size* - The number of data points written to the data store during the last write.
* *kairosdb.datastore.write_buffer_size* - The size of the write buffer at the time of the last write.
* *kairosdb.protocol.http_request_count* - The number of HTTP requests for each method. This includes a method tag that indicates the method that was called. For example, method=query if a query was done.
* *kairosdb.protocol.telnet_request_count* - The number of telnet requests for each method. This includes a method tag that indicates the method that called. For example, method=put if the put method was called.
* *kairosdb.jvm.free_memory* - The amount of free memory available in the JVM.
* *kairosdb.jvm.total_memory* - The amount of total memory in the JVM.
* *kairosdb.jvm.max_memory* - The maximum amount of memory the JVM will attempt to use.
* *kairosdb.jvm.thread_count* - The total number of threads running in the JVM.