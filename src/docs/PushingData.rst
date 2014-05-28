============
Pushing data
============

You can submit data either with the telnet protocol on port 4242 or the rest protocol on port 8080 (ports can be changed in the kairosdb.properties file)

A great client for testing data in and out is [http://code.google.com/p/rest-client/]

--------------------------
Submitting data via telnet
--------------------------

The format of the data is 
::

	put <metric name> <time stamp> <value> <tag> <tag>... /n

**Metric name** must be one word and is limited to alpha numerics with "-_.".

**Time stamp** can either be in milliseconds or in seconds since Jan 1, 1970 (unix epoch) - seconds are for backwards compatibility with the original opentsdb.  The Cassandra datastore supports milliseconds where as the HBase datastore only supports seconds.

	**Note:** The REST API only supports a timestamp in milliseconds.

**Value** can either be a long or double value.

**Tag** is in the form of key=value.

Be aware that the data sent must be followed by a line feed character.

Here is a simple shell script that inserts data using netcat.
::

	#!/bin/bash

	# Current time in milliseconds
	now=$(($(date +%s%N)/1000000))
	metric=load_value_test
	value=42
	host=10.92.4.4

	echo "put $metric $now $value host=A" | nc -w 30 $host 4242


------------------------
Submitting data via rest
------------------------

The url for submitting data is http://localhost:8080/api/v1/datapoints
::

	[{
	    "name": "archive.file.tracked",
	    "timestamp": 1349109376,
	    "value": 123,
	    "tags":{"host":"test"}
	},
	{
	    "name": "archive.file.search",
	    "timestamp": 999,
	    "value": 321,
	    "tags":{"host":"test"}
	}]

In the case of the rest api the timestamp is always treated as milliseconds since Jan 1, 1970.  If you are writing to HBase it is truncated off to the second.

See the :doc:`REST API documentation <restapi/AddDataPoints>` for more details

-----------------
Graphite protocol
-----------------

KairosDB now supports the Graphite plaintext and pickle protocol as explained [https://graphite.readthedocs.org/en/latest/feeding-carbon.html here].  This lets you integrate KairosDB with existing applications that push data to Graphite.