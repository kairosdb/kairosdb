==========================
Frequently Asked Questions
==========================

--------------------------
Why would I pre-aggregate?
--------------------------

Lets say you have a graph of your data that shows 1 hour averages of your data for a week.  You update this graph several times a day, each time you do the code queries a week of data and aggregates it.  Each time you run the query you are re processing 99% of the same data you processed the last time you ran the query.  What can be done?

There are several strategies here:

1.  Pre-aggregate the data before it is written.  The challenge here is that not all of the data is necessarily coming in through the same K* node.  In a load balanced system incoming metrics are spread across several K* nodes so aggregating on the K* node doesn't really work.
2.  Only query the new data.  The idea here is that your visualization tool is smart enough to only query the new data and merge it into the graph.  Cubism.js is a tool that claims such functionality.
3.  Batch jobs that pre-aggregate and write to a new metric.  The new metric can then be queried and used to graph the data.  This could be done with a cron job.  There is some discussion about making the batch job part of K* but, there is work to be done before this can happen.

------------------------------------------------------------
Why can tags only handle ascii characters, '-', '_' and '/'?
------------------------------------------------------------

This is a carry over from supporting hbase/opentsdb.  There is no reason this has to stay the way it is and will be changed in a future release.


