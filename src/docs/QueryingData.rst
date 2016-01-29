=============
Querying data
=============

The URL for getting data out is http://localhost:8080/api/v1/datapoints/query

Getting data out with an absolute date

.. code-block:: json

	{
	  "start_absolute":1,
	  "metrics": [
	    {
	      "name": "archive.file.tracked"
	    }
	  ]
	}


Getting data out with a relative date

.. code-block:: json

  {
    "start_relative":{"value":20,"unit":"weeks"},
    "metrics": [
      {
        "name": "archive.file.tracked"
      }
    ]
  }


You can also specify a end_absolute and end_relative in the same format as start.  If the end is not specified then the end is assumed to be now.

See the :doc:`REST API documentation<restapi/QueryMetrics>` for more details.
