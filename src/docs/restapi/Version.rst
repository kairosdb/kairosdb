=======
Version
=======

Returns the version of KairosDB. 

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/version

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
    "version": "KairosDB 0.9.4"
  }

