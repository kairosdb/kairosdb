=============
Query Metrics
=============

Returns a list of metric values based on a set of criteria. Also returns a set of all tag names and values that are found across the data points.

The time range can be specified with absolute or relative time values. Absolute time values are in milliseconds.
Relative time values are specified as an integer duration and a unit. Possible unit values are "milliseconds", "seconds", "minutes", "hours",
"days", "weeks", "months", and "years". For example, "5 hours" means that metric values submitted 5 hours ago will be returned. The end time is
optional.  If no end time is specified, the end time is assumed to be now (the current date and time).

**Grouping**

The results of the query can be grouped together.There are three ways to group the data; by tags, by a time range, and by value. Grouping is done with the *group_by* property which is an array of one or more groupers.

Note that grouping by a time range or value can slow down the query.

**Aggregators**

Optionally you can specify aggregators. Aggregators perform an operation on data
points and down samples. For example, you could sum all data points that exist in 5 minute periods.

Aggregators can be combined together. For example, you could sum all data points
in 5 minute periods then average them for a week period.

Aggregators are processed in the order they are specified in the JSON. The output of one is send to the input of the next.

See the :doc:`Aggregators documentation<Aggregators>` for a complete list of aggregators.

**Filtering**

It is possible to filter the data returned by specifying a tag. The data returned will only contain data points associated with the specified tag. Filtering is done using the "tags" property.

---------------
Request Methods
---------------

Queries can be done using either a GET or POST method.

--------------------------------------------------------------------------------------------

The GET version requires that the JSON is encoded and passed to the "query" parameter.

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/datapoints/query?query=[encoded_JSON]

----
Body
----
  NONE

--------------------------------------------------------------------------------------------

The POST version takes the query JSON in the body of the request.

------
Method
------

  POST

-------
Request
-------

  http://[host]:[port]/api/v1/datapoints/query

----
Body
----

.. code-block:: json

   {
      "start_absolute": 1357023600000,
      "end_relative": {
          "value": "5",
          "unit": "days"
      },
      "time_zone": "Asia/Kabul",
      "metrics": [
          {
              "tags": {
                  "host": ["foo", "foo2"],
                  "customer": ["bar"]
              },
              "name": "abc.123",
              "limit": 10000,
              "aggregators": [
                  {
                      "name": "sum",
                      "sampling": {
                          "value": 10,
                          "unit": "minutes"
                      }
                  }
              ]
          },
          {
              "tags": {
                  "host": ["foo", "foo2"],
                  "customer": ["bar"]
              },
              "name": "xyz.123",
              "aggregators": [
                  {
                      "name": "avg",
                      "sampling": {
                          "value": 10,
                          "unit": "minutes"
                      }
                  }
              ]
          }
      ]
   }

----------------
Query Properties
----------------

You must specify either *start_absolute* or *start_relative* but not both. Similarly, you may specify either *end_absolute* or *end_relative* but not both. If either end time is not specified the current date and time is assumed.

*start_absolute* 
The time in milliseconds.

*start_relative*
The relative start time is the current date and time minus the specified value and unit. Possible unit values are "milliseconds", "seconds", "minutes", "hours", "days", "weeks", "months", and "years". For example, if the start time is 5 minutes, the query will return all matching data points for the last 5 minutes.

*end_absolute* 
The time in milliseconds. This must be later in time than the start time. If not specified, the end time is assumed to be the current date and time.

*end_relative*
The relative end time is the current date and time minus the specified value and unit. Possible unit values are "milliseconds", "seconds", "minutes", "hours", "days", "weeks", "months", and "years". For example, if the start time is 30 minutes and the end time is 10 minutes, the query returns matching data points that occurred between the last 30 minutes up to and including the last 10 minutes. If not specified, the end time is assumed to the current date and time.

*time_zone*
The time zone for the time range of the query. If not specified, UTC is used.

*cache_time*
	The amount of time in seconds to re use the cache from a previous query. When a query is made Kairos looks for the cache file for the query.  If a cache file is found and the timestamp of the cache file is within cache_time seconds from the current query, the cache is used.

	Cache files are identified by hashing the metric name, the start and end time of the query and any tags specified.  For example if you query a metric using relative start of 4 hours ago and then 30 min later you run the same query with a cache_time set to 2000 (just over 30 min) you will get the cached data back.

	Sending a query with a cache_time set to 0 will always refresh the cache with new data from Cassandra.

	Changing aggregators on a query does not effect the use of cache.

-----------------
Metric Properties
-----------------

*name*

The name of the metric(s) to return data points for. The name is required.

*aggregators*

This is an ordered array of aggregators. They are processed in the order specified.
The output of an aggregator is passed to the input of the next until all have been processed.

If no aggregator is specified, then all data points are returned.

Most aggregators support downsampling. Downsampling allows you to reduce the sampling rate of the data points and aggregate these values over a longer period
of time. For example, you could average all daily values over the last week. Rather than getting 7 values you would
get one value which is the average for the week. Sampling is specified with a "value" and a "unit".

* value - An integer value.
* unit - The time range. Possible unit values are "milliseconds", "seconds", "minutes", "hours", "days", "weeks", "months", and "years".
* align_sampling - An optional property. Setting this to true will cause the aggregation range to be aligned based on the sampling size.  For example if your sample size is either milliseconds, seconds, minutes or hours then the start of the range will always be at the top of the hour.  The effect of setting this to true is that your data will take the same shape when graphed as you refresh the data. This is false by default. *Note that align_sampling and align_start_time are mutually exclusive. If more than one are set, unexpected results will occur.*
* align_start_time - An optional property. When set to true the time for the aggregated data point for each range will fall on the start of the range instead of being the value for the first data point within that range. This is false by default. *Note that align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.*
* align_end_time - An optional property. Setting this to true will cause the aggregation range to be aligned based on the sampling size. For example if your sample size is either milliseconds, seconds, minutes or hours then the start of the range will always be at the top of the hour. The difference between align_start_time and align_end_time is that align_end_time sets the timestamp for the datapoint to the beginning of the following period versus the beginning of the current period. As with align_start_time, setting this to true will cause your data to take the same shape when graphed as you refresh the data. Note that align_start_time and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.
* start_time - An optional property. Used along with align_start_time. This is the alignment start time. This defaults to 0.


*tags*

Tags narrow down the search. Only metrics that include the tag and matches one of the values are returned. Tags is optional.

*group_by*

The resulting data points can be grouped by one or more tags, a time range, or by value, or by a combination of the three.

The "group_by" property in the query is an array of one or more groupers. Each grouper has a *name* and then additional properties specific to that grouper.

See :doc:`Grouping by Tags <TagGrouping>` for information on grouping by tags.

See :doc:`Grouping by Time <TimeGrouping>` for information on how to group by a time range.

See :doc:`Grouping by Value <ValueGrouping>` for information on how to group by data point values.

See :doc:`Grouping by Bins <BinGrouping>` for information on how to group by bins.


Note that grouping by a time range, by value, or by bins can slow down the query.

*exclude_tags*

By default, the result of the query includes tags and tag values associated with the data points. If *exclude_tags* is set to true, the tags will be excluded from the response.

*limit*

Limits the number of data points returned from the data store. The limit is applied before any aggregator is executed.

*order*

Orders the returned data points. Values for *order* are "asc" for ascending or "desc" for descending. Defaults to ascending. This
 sorting is done before any aggregators are executed.

--------
Response
--------
*Success*

  The response contains either the metric values or possible error values. Returns 200 for successful queries.

  Version 0.9.4 includes a group_by named "type". The type is the custom data type. If the data returned is not a custom
  type then "number" is returned. See :doc:`Custom Types <../kairosdevelopment/CustomData>` for
  information on custom types.

  .. code-block:: json

     {
       "queries": [
           {
               "sample_size": 14368,
               "results": [
                   {
                       "name": "abc_123",
                       "group_by": [
                           {
                              "name": "type",
                              "type": "number"
                           },
                           {
                              "name": "tag",
                              "tags": [
                                  "host"
                              ],
                             "group": {
                                  "host": "server1"
                             }
                           }
                       ],
                       "tags": {
                           "host": [
                               "server1"
                           ],
                           "customer": [
                               "bar"
                           ]
                       },
                       "values": [
                           [
                               1364968800000,
                               11019
                           ],
                           [
                               1366351200000,
                               2843
                           ]
                       ]
                   }
              ]
          }
       ]
     }


*Failure*

  The response will be 400 Bad Request if the request is invalid.

  The response will be 500 Internal Server Error if an error occurs retrieving data.

  .. code-block:: json

     {
         "errors": [
             "metrics[0].aggregate must be one of MIN,SUM,MAX,AVG,DEV",
             "metrics[0].sampling.unit must be one of  SECONDS,MINUTES,HOURS,DAYS,WEEKS,YEARS"
         ]
     }
