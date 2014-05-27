==================
Delete Data Points
==================

Delete will perform the query specified in the body and delete all data points returned by the query. Aggregators and groupers have no effect on which data points are deleted.

Delete is designed such that you could perform a query, verify that the data points returned are correct, and issue the delete with that query.

Note: Delete works for the Cassandra and H2 data store only.

------
Method
------
  Post

-------
Request
-------

  http://[host]:[port]/api/v1/datapoints/delete

----
Body
----

  A query. See the documentation on [QueryMetrics query].

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
      "errors":["Connect to 10.92.4.1:4242 timed out"]
    }
