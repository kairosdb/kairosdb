========
Overview
========

This API provides an operation to store metrics and query for the current version of Kairosdb.

Data points have a metric name, a value, a timestamp, and a list of one or more tags. Tags are named properties that identify the data, such as its type and where it comes from.

Metric names, tag names and values are case sensitive and can only contain the following characters: alphanumeric characters, period ".", slash "/", dash "-", and underscore "`_`".  

If a data point names a metric that does not exist, the metric is created.

The timestamp is the number of milliseconds since January 1st, 1970 UTC.