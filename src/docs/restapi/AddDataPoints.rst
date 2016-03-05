---------------
Add Data Points
---------------

Records metric data points

^^^^^^
Method
^^^^^^

  POST

^^^^^^^
Request
^^^^^^^

  http://[host]:[port]/api/v1/datapoints

*Note: you can gzip the json and upload with the content type set to application/gzip if you are batching large amounts of data.*

^^^^
Body
^^^^
::

  [
    {
        "name": "archive_file_tracked",
        "datapoints": [[1359788400000, 123], [1359788300000, 13.2], [1359788410000, 23.1]],
        "tags": {
            "host": "server1",
            "data_center": "DC1"
        },
        "ttl": 300
    },
    {
        "name": "impedance",
        "type": "complex-number",
        "datapoints": [
            [
                1359788400000,
                {
                    "real": 2.3,
                    "imaginary": 3.4
                }
            ],
            [
                1359788300000,
                {
                    "real": 1.1,
                    "imaginary": 5
                }
            ]
        ],
        "tags": {
            "host": "server1",
            "data_center": "DC1"
        }
    },
    {
        "name": "archive_file_search",
        "timestamp": 1359786400000,
        "value": 321,
        "tags": {
            "host": "server2"
        }
    }
  ]

^^^^^^^^^^^
Description
^^^^^^^^^^^

You can either use **"timestamp"** with **"value"** for a single data point or you can use **"datapoints"** to post multiple data points. This example shows both approaches.

**name**

Metric names must be unique. Multiple words in a metric name are typically separated using an underscore ("_") to separate words such as archive_search.

**timestamp**

The timestamp is the date and time when the data was measured. It's a numeric value that is the number of milliseconds since January 1st, 1970 UTC.

**value**

The value is a number (i.e, 523 or 132.45).

**datapoints**

An array of data points. Each data point consists of a timestamp and value.

**tags**

The tags field is a list of named properties. At least one tag is required. The tags are used when querying
metrics to narrow down the search. For example, if multiple metrics are measured on server1, you could
add the "host":"server1" tag to each of the metrics and queries could return all metrics for the "host"
tagged with the value of "server1".

**type**

Type identifies custom data types. This field is only needed if the data value is something other than a number.
The type field is the name of the registered type for the custom data. See :doc:`Custom Types <../kairosdevelopment/CustomData>` for information on custom types.

**ttl**

Sets the Cassandra ttl for the data points.  In the example above the data points for metric ``archive_file_tracked``
will have the ttl set for 5 min.  Leaving the ttl off or setting it to 0 will use the default TTL value specified in
settings.


^^^^^^^^
Response
^^^^^^^^
*Success*
  The response will be 204 NO CONTENT with no body.

*Failure Response*

  The response will be 400 Bad Request if the request is invalid.

  The response will be 500 Internal Server Error if an error occurs.
  ::

    {
      "errors": [
        "Connect to 10.92.4.1:4242 timed out"
      ]
    }

