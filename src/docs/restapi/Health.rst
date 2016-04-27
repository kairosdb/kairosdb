=============
Health Checks
=============

KairosDB provides REST APIs that show the health of the system.

There are currently two health checks executed for each API.

* The JVM thread deadlock check verifies that no deadlocks exist in the KairosDB JVM.
* The Datastore query check performs a query on the data store to ensure that the data store is responding.

------
Status
------

Returns the status of each health check as JSON.

""""""
Method
""""""

  GET

"""""""
Request
"""""""

  http://[host]:[port]/api/v1/health/status

""""""""
Response
""""""""

  Always returns 200.

  ::

  ["JVM-Thread-Deadlock: OK","Datastore-Query: OK"]

-----
Check
-----

Checks the status of each health check. If all are healthy it returns status 204 otherwise it returns 500.
This can be configured to return something other than 204 by changing the kairosdb.health.healthyResponseCode property.

""""""
Method
""""""

  GET

"""""""
Request
"""""""

  http://[host]:[port]/api/v1/health/check

""""""""
Response
""""""""

*Success*
  Returns 204 if all checks are healthy.

*Failure*
  Returns 500 if any of the checks are unhealthy.
