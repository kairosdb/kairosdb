=================
List Metric Names
=================

Returns a list of all metric names. If you specify the prefix parameter, only
names that start with prefix are returned.

------
Method
------
  GET

-------
Request
-------

  http://[host]:[port]/api/v1/metricnames

  http://[host]:[port]/api/v1/metricnames?prefix=[prefix]

----
Body
----

  None

--------
Response
--------
*Success*
  Returns 200 for successful queries.
  ::

    {
      "results": [
        "archive_file_search",
        "archive_file_tracked"
      ]
    }


