========
Roll-ups
========

Roll-ups are a way to improve query performance by aggregating or rolling up data into a larger time range such as averaging millisecond data over a minute.
Roll-ups perform queries on existing data, aggregate the data, and write the results to another metric. The original data is left intact.
Roll-ups are continuously run on a schedule rolling up a small time period of data on each execution.
Roll-ups are scheduled by creating a roll-up task. A task contains one or more roll-ups and an execution interval (how often the task is executed).
A roll-up is a query (see :doc:`QueryMetrics <QueryMetrics>`) and the name of a new metric where it will be saved (save_as).

--------------------------------------------------------------------------------------------

===================
Create Roll-up Task
===================
A roll-up task is executed on a schedule to create one or more roll-ups.

------
Method
------

  POST

-------
Request
-------

  http://[host]:[port]/api/v1/rollups

----
Body
----

::

 {
 	"name": "MyRollup",
 	"execution_interval": {
 		"value": 1,
 		"unit": "hours"
 	},
 	"rollups": [{
 		"save_as": "kairosdb.http.query_time_rollup",
 		"query": {
 			"cache_time": 0,
 			"start_relative": {
 				"value": "1",
 				"unit": "hours"
 			},
 			"metrics": [{
 				"name": "kairosdb.http.query_time",
 				"limit": 10000,
 				"tags": {
 					"host": ["foo", "bar"],
 					"customer": ["foobar"]
 				},
 				"aggregators": [{
 					"name": "sum",
 					"sampling": {
 						"value": 1,
 						"unit": "minutes"
 					}
 				}, {
 					"name": "avg",
 					"sampling": {
 						"value": 10,
 						"unit": "minutes"
 					}
 				}]
 			}]
 		}
 	}]
 }

------------------
Roll-up Properties
------------------

*name*
The name of the roll-up task.

*execution_interval*
When the roll-up task is exected specified as value and unit. Possible unit values are "seconds", "minutes", "hours", "days", "weeks", "months", and "years".

*rollups*
An array of roll-ups. A roll-up consists of

* *save_as* - the name of the metric where the roll-up data points are stored.
* *query* - the query that is performed (see :doc:`QueryMetrics <QueryMetrics>`).


--------
Response
--------
*Success*

  The response contains either the metric resource created. Returns 200 when successful.

  ::

    {
        "id": "7b2893fc-7654-4764-851a-a311e854ee75",
        "name": "MyRollupTaskName",
        "attributes": {
            "url": "/api/v1/rollups/7b2893fc-7654-4764-851a-a311e854ee75"
        }
    }

*Failure*

  The response will be 400 Bad Request if the request is invalid.

  The response will be 500 Internal Server Error if an error occurs retrieving data.

  ::

    {
        "errors": [
            "metrics[0].aggregate must be one of MIN,SUM,MAX,AVG,DEV",
            "metrics[0].sampling.unit must be one of  SECONDS,MINUTES,HOURS,DAYS,WEEKS,YEARS"
        ]
    }

==================
List Roll-up Tasks
==================
Lists all defined roll-up tasks.

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/rollups

----
Body
----

	None

--------
Response
--------
*Success*

  The response contains a list of roll-up tasks. Returns 200 for success.

  ::

    [{
    	"id": "ce55e623-0610-4451-a725-3daee896afd9",
    	"name": "MyRollup",
    	"execution_interval": {
    		"value": 1,
    		"unit": "hours"
    	},
    	"rollups": [{
    		"save_as": "kairosdb.http.query_time_rollup",
    		"query": {
    			"cache_time": 0,
    			"start_relative": {
    				"value": "1",
    				"unit": "hours"
    			},
    			"metrics": [{
    				"name": "kairosdb.http.query_time",
    				"limit": 10000,
    				"tags": {
    					"host": ["foo", "bar"],
    					"customer": ["foobar"]
    				},
    				"aggregators": [{
    					"name": "sum",
    					"sampling": {
    						"value": 1,
    						"unit": "minutes"
    					}
    				}, {
    					"name": "avg",
    					"sampling": {
    						"value": 10,
    						"unit": "minutes"
    					}
    				}]
    			}]
    		}
    	}]
    }]

*Failure*

  The response will be 500 Internal Server Error if an error occurs retrieving data.


================
Get Roll-up Task
================
Returns the roll-up task by Id.

------
Method
------

  GET

-------
Request
-------

  http://[host]:[port]/api/v1/rollups/{id}

----
Body
----

	None

--------
Response
--------
*Success*

  The response contains the roll-up for the given id. Returns 200 for success.

  ::

    {
    	"id": "ce55e623-0610-4451-a725-3daee896afd9",
    	"name": "MyRollup",
    	"execution_interval": {
    		"value": 1,
    		"unit": "hours"
    	},
    	"rollups": [{
    		"save_as": "kairosdb.http.query_time_rollup",
    		"query": {
    			"cache_time": 0,
    			"start_relative": {
    				"value": "1",
    				"unit": "hours"
    			},
    			"metrics": [{
    				"name": "kairosdb.http.query_time",
    				"limit": 10000,
    				"tags": {
    					"host": ["foo", "bar"],
    					"customer": ["foobar"]
    				},
    				"aggregators": [{
    					"name": "sum",
    					"sampling": {
    						"value": 1,
    						"unit": "minutes"
    					}
    				}, {
    					"name": "avg",
    					"sampling": {
    						"value": 10,
    						"unit": "minutes"
    					}
    				}]
    			}]
    		}
    	}]
    }

*Failure*

  The response will be 404 if the roll-up resource specified does not exist.

  The response will be 500 Internal Server Error if an error occurs retrieving data.

===================
Delete Roll-up Task
===================
Deletes the roll-up task specified for the given Id.

------
Method
------

  DELETE

-------
Request
-------

  http://[host]:[port]/api/v1/rollups/{id}

----
Body
----

	None

--------
Response
--------
*Success*

  No data is returned. Returns 204 if the task was successfully deleted.

*Failure*

  The response will be 404 if the roll-up resource specified does not exist.

  The response will be 500 Internal Server Error if an error occurs retrieving data.

===================
Update Roll-up Task
===================
Updates the roll-up task specified by the Id.

------
Method
------

  PUT

-------
Request
-------

  http://[host]:[port]/api/v1/rollups/{id}

----
Body
----

  ::

    {
	    "name": "MyRollup",
	    "execution_interval": {
		    "value": 1,
		    "unit": "hours"
	    },
	    "rollups": [{
		    "save_as": "kairosdb.http.query_time_rollup",
		    "query": {
			    "cache_time": 0,
			    "start_relative": {
				    "value": "1",
				    "unit": "hours"
			    },
			    "metrics": [{
				    "name": "kairosdb.http.query_time",
				    "limit": 10000,
				    "tags": {
					    "host": ["foo", "bar"],
					    "customer": ["foobar"]
				    },
				    "aggregators": [{
					    "name": "sum",
					    "sampling": {
						    "value": 1,
						    "unit": "minutes"
					    }
				    }, {
					    "name": "avg",
					    "sampling": {
						    "value": 10,
						    "unit": "minutes"
					    }
				    }]
			    }]
		    }
	    }]
     }

--------
Response
--------
*Success*

  The response contains the roll-up for the given id. Returns 200 for success.

  ::

    {
    	"id": "ce55e623-0610-4451-a725-3daee896afd9",
    	"name": "MyRollup",
    	"execution_interval": {
    		"value": 1,
    		"unit": "hours"
    	},
    	"rollups": [{
    		"save_as": "kairosdb.http.query_time_rollup",
    		"query": {
    			"cache_time": 0,
    			"start_relative": {
    				"value": "1",
    				"unit": "hours"
    			},
    			"metrics": [{
    				"name": "kairosdb.http.query_time",
    				"limit": 10000,
    				"tags": {
    					"host": ["foo", "bar"],
    					"customer": ["foobar"]
    				},
    				"aggregators": [{
    					"name": "sum",
    					"sampling": {
    						"value": 1,
    						"unit": "minutes"
    					}
    				}, {
    					"name": "avg",
    					"sampling": {
    						"value": 10,
    						"unit": "minutes"
    					}
    				}]
    			}]
    		}
    	}]
    }

*Failure*

  The response will be 400 Bad Request if the request is invalid.

  The response will be 404 if the roll-up resource specified does not exist.

  The response will be 500 Internal Server Error if an error occurs retrieving data.
