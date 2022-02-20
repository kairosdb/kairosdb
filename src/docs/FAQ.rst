##########################
Frequently Asked Questions
##########################

=====================================
Why do I need a Time-Series database?
=====================================

Time series databases store scalar values over time, often with additional meta data (referred to as tags).  In comparison to a relational database where a row in a table would typically model a single scalar value at a specific time, time series databases typically collapse many scalar values into a single row that instead represents a period of time.

Databases are required to be able to both store and retrieve records but in a timely manner. Typically an index is used on a table to enable fast retrieval by reducing disk I/O.  An index is only efficient if it is small enough to fit into memory, because if it is not it will have to be paged to disk requiring I/O.  Since the size of an index is proportional is the number of rows in the table, time series databases are able to perform much better than relational stores because they have less rows.

==========================
Why would I pre-aggregate?
==========================

Lets say you have a graph of your data that shows 1 hour averages of your data for a week.  You update this graph several times a day, each time you do the code queries a week of data and aggregates it.  Each time you run the query you are re processing 99% of the same data you processed the last time you ran the query.  What can be done?

There are several strategies here:

1.  Pre-aggregate the data before it is written.  The challenge here is that not all of the data is necessarily coming in through the same K* node.  In a load balanced system incoming metrics are spread across several K* nodes so aggregating on the K* node doesn't really work.
2.  Only query the new data.  The idea here is that your visualization tool is smart enough to only query the new data and merge it into the graph.  Cubism.js is a tool that claims such functionality.
3.  Batch jobs that pre-aggregate and write to a new metric.  The new metric can then be queried and used to graph the data.  This could be done with a cron job.  There is some discussion about making the batch job part of K* but, there is work to be done before this can happen.


Additional FAQ can be found here: https://github.com/kairosdb/kairosdb/wiki/Frequently-Asked-Questions

===============================
Can I run Kairos in Kubernetes?
===============================

KairosDB runs just fine under Kubernetes.  Kairos for the most part is stateless which lends itself to be easily deployed in Kubernetes.  Cassandra on the other hand is not so easy.  Many people have tried and failed to get Cassandra to work efficiently in Kubernetes.
