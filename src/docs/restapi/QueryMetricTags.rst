=================
Query Metric Tags
=================

You can think of this as the exact same as the query but it leaves off the data and just returns the tag information.  

Note: Currently this is not implemented in the HBase datastore.

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

::

 {
    "start_absolute": 1357023600000,
    "end_relative": {
        "value": "5",
        "unit": "days"
    },
    "metrics": [
        {
            "tags": {
                "host": ["foo"]
            },
            "name": "abc.123",
        },
        {
            "tags": {
                "host": ["foo"]
            },
            "name": "xyz.123",
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

::

  {
      "results": [
          {
              "name": "abc_123",
              "tags": {
                  "host": ["server1","server2"],
                  "type": ["bar"]
              }
          },
          {
              "name": "xyz_123",
              "tags": {
                  "host": ["server1","server2"],
                  "type": ["bar"]
              }
          }
      ]
  }

*Failure*

  The response will be 400 Bad Request if the request is invalid.

  The response will be 500 Internal Server Error if an error occurs retrieving data.