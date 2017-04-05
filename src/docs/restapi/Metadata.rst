========
Metadata
========

The Metadata Rest API is a way to write data to the datastore in name/value pairs. Data is written
separate from the time series data. Metadata is partitioned by a service name. A service partition
can have multiple service keys. Each service key holds name/value pairs. A value is a string.

**Example**

Assume you have a service that maintains metadata about each metric. Let's call it the Metric Service.
Your service associates each metric with a description, owner, and the unit type. The service name
is "Metric Service", the metric is the service key and the name/value pairs are the owner, unit, and
description and their values.

**Metric Service**

+------------------------+----------+---------+---------------------+
| Metric                 | Owner    | Unit    | Description         |
+========================+==========+=========+=====================+
| disk.available         | OPs team | MB      | Available disk space|
+------------------------+----------+---------+---------------------+
| foo.throughput         | Foo team | Bytes   |  Number of bytes    |
+------------------------+----------+---------+---------------------+


--------------------------------------------------------------------------------------------

=============
Add the Value
=============
Add a value to service metadata.

------
Method
------

  POST

-------
Request
-------

  http://[host]:[port]/api/v1/metadata/{service}/{serviceKey}/{key}

----
Body
----

::

  <The Value>



-----------
Description
-----------
Writes the value for the given service, service key, and key.

*service*
The name of the service.

*serviceKey*
The name of the service key.

*key*
The name of the key.

*value*
The value to store. The value must be a string.

--------
Response
--------
*Success*

  Returns 204 when successful.

*Failure*

  The response will be 500 Internal Server Error if an error occurs writing the value.

  ::

    {
        "errors": [
            "Failed to add value"
        ]
    }



=============
Get the Value
=============
Returns the value for the given service.

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/metadata/{service}/{serviceKey}/{key}

----
Body
----

  None

-----------
Description
-----------
Returns the value for the given service, service key, and key if it exists or an empty response if it
does not exist.

*service*
The name of the service.

*serviceKey*
The name of the service key.

*key*
The name of the key.

--------
Response
--------

*Success*

  The response contains the value or an empty string if not found. Returns 200 when successful.

*Failure*

  The response will be 500 Internal Server Error if an error occurs writing the value.

  ::

    {
        "errors": [
            "Failed to retrieve value"
        ]
    }


=================
List Service Keys
=================
Returns all service keys for the given service

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/metadata/{service}

----
Body
----

  None

-----------
Description
-----------
Returns all keys for the given service or an empty list if no service keys exist for the given service.

*service*
The name of the service.

--------
Response
--------

*Success*

  ::

     {
        "results":["service_key_1", "service_key_2"]
     }

  The response contains a list of service keys for the given service or an empty string if not found. Returns 200 when successful.

*Failure*

  The response will be 500 Internal Server Error if an error occurs writing the value.

  ::

    {
        "errors": [
            "Failed to get keys"
        ]
    }

=========
List Keys
=========
Returns all keys for the given service and service key.

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/metadata/{service}/{serviceKey}

----
Body
----

  None

-----------
Description
-----------
Returns all keys for the given service key or an empty list if no keys exist.

*service*
The name of the service.

*serviceKey*
The name of the service key.

--------
Response
--------

*Success*

  ::

     {
        "results":["key_1", "key_2"]
     }

  The response contains a list of keys for the given service key or an empty string if not found. Returns 200 when successful.

*Failure*

  The response will be 500 Internal Server Error if an error occurs writing the value.

  ::

    {
        "errors": [
            "Failed to get keys"
        ]
    }


==========
Delete Key
==========
Deletes the specified key.

------
Method
------

  DELETE

-------
Request
-------

  http://[host]:[port]/api/v1/metadata/{service}/{serviceKey}/{key}

----
Body
----

  None

-----------
Description
-----------
Returns all keys for the given service key or an empty list if no keys exist.

*service*
The name of the service.

*serviceKey*
The name of the service key.

--------
Response
--------

*Success*

  ::

     {
        "results":["key_1", "key_2"]
     }

  The response contains a list of keys for the given service key or an empty string if not found. Returns 200 when successful.

*Failure*

  The response will be 500 Internal Server Error if an error occurs writing the value.

  ::

    {
        "errors": [
            "Failed to delete key"
        ]
    }
