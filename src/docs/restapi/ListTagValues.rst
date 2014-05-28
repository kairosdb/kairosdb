===============
List Tag Values
===============

Returns a list of all tag values.

------
Method
------

GET

-------
Request
-------

  http://[host]:[port]/api/v1/tagvalues

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
          "server1",
          "bar"
      ]
    }
