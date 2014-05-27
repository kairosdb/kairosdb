==============
List Tag Names
==============

Returns a list of all tag names.

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/tagnames

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
        "host",
        "type"
      ]
    }


