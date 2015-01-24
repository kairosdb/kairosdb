=============
Delete Metric
=============

Deletes a metric and all data points associated with the metric.

Note: Delete works for the Cassandra and H2 data stores only. 

------
Method
------
  Delete

-------
Request
-------

  http://[host]:[port]/api/v1/metric/{metric_name}

----
Body
----

  None

--------
Response
--------
*Success*

  The response will be 204 NO CONTENT with no body.

*Failure*

  The response will be 400 Bad Request if the request is invalid.

  The response will be 500 Internal Server Error if an error occurs.
  ::

    {
      "errors": ["Connect to 10.92.4.1:4242 timed out"]
    }
