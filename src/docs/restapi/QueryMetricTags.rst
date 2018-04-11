=================
Query Metric Tags
=================

Similar to a query but only returns the tags (no data points returned). This can potentially return more tags than a
query because it is optimized for speed and does not query all rows to narrow down the time range. This queries only
the Row Key Index and thus the time range is the starting time range. Since the Cassandra row is set to 3 weeks, this
can return tags for up to a 3 week period. See :doc:`Cassandra Schema </CassandraSchema>`.

---------
Filtering
---------
It is possible to filter the tags returned by specifying a tag.

------
Method
------

  POST

-------
Request
-------

  http://[host]:[port]/api/v1/datapoints/query/tags

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
    "metrics": [
        {
            "tags": {
                "host": ["server1"]
            },
            "name": "abc_123"
        },
        {
            "tags": {
                "dc": ["awsuse"]
            },
            "name": "xyz_123"
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

-----------------
Metric Properties
-----------------

*name*

The name of the metric(s) to return data points for. The name is required.

*tags*

Tags narrow down the search. Only metrics that include the tag and matches one of the values are returned. Tags is optional. 

--------
Response
--------
*Success*
  The response contains either the metric values or possible error values. Returns 200 for successful queries.

  .. code-block:: json

    {
        "results": [
            {
                "name": "abc_123",
                "tags": {
                    "host": ["server1"],
                    "dc": ["awsuse", "awsusw"],
                    "type": ["bar"]
                },
                "values": [[1492602706055,0],[1492602711000,0],[1492602712000,0],[1492602716055,0]]
            },
            {
                "name": "xyz_123",
                "tags": {
                    "host": ["server1","server2"],
                    "dc": ["awsuse"],
                    "type": ["bar"]
                },
                "values": [[1492602706055,0],[1492602711000,42],[1492602712000,0],[1492602716055,42]]
            }
        ]
    }

*Failure*

  The response will be 400 Bad Request if the request is invalid.

  The response will be 500 Internal Server Error if an error occurs retrieving data.
