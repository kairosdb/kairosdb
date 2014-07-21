=================
List Metric Names
=================

Returns a list of all metric names.

------
Method
------
  GET

-------
Request
-------

  http://[host]:[port]/api/v1/metricnames

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


