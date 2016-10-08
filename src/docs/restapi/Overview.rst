########
Overview
########

This API provides operations to list existing metric names, list tag names and values, store metric data points, and query for metric data points.

Data points have a metric name, a value, a timestamp, and a list of one or more tags. Tags are named properties that identify the data, such as its type and where it comes from.

Metric names, tag names and values are case sensitive.

If a data point names a metric that does not exist, the metric is created.

The timestamp is the number of milliseconds since January 1st, 1970 UTC. 

You can query for data points by specifying their metric name and a time range, and optionally one or more tags. A query can perform data manipulation operations such as aggregation, averaging, min and max calculations, and downsampling.

All posts and responses are in JSON format including error messages.

---------------------
What Data do I Store?
---------------------

Don't store rate calculations or percentages. Send the actual numbers. Sending rates and percentages limit the amount of useful information that could otherwise be obtained from the raw values.

If there is too much data to store as raw numbers, one approach at aggregation is to send a count of the data in buckets.
For example, if you want to track the amount of time (in seconds) a request took, you could create 5 buckets in 10 second
increments. Each time a request is performed, you add one to the bucket for that time interval. The metric values you
then post are the counts for each bucket.

