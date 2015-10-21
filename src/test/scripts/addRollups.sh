#!/bin/bash

curl -s http://localhost:8080/api/v1/rollups/rollup -XPOST -H'Content-type:application/json' -d '
	{
		"name": "jeff_rollup3",
        "schedule": "0 * * * * ?",
        "rollups": [
         {
	         "save_as": "rolluptest1",
             "query":
             {
              "cache_time": 0,
			   "start_relative": {
                    "value": "1",
                    "unit": "hours"
                },
			   "metrics": [
                {
                    "name": "kairosdb.http.query_time",
                    "limit": 10000,
                    "tags": {
						"host": ["foo", "bar"],
						"customer": ["foobar"]
					},

                    "aggregators": [
                    {
                        "name": "sum",
                        "sampling": {
                            "value": 10,
                            "unit": "minutes"
                        }
                    },
                    {
                        "name": "avg",
                        "sampling": {
                            "value": 10,
                            "unit": "hours"
                          }
                     }
                    ]
                }]
			 }
         },
         {
	         "save_as": "rolluptest2",
	         "tags": {},
             "query":
             {
              "cache_time": 0,
			   "start_relative": {
                    "value": "1",
                    "unit": "hours"
                },
			   "metrics": [
                {
                    "name": "kairosdb.telnet.query_time",
                    "limit": 10000,
                    "aggregators": [
                    {
                        "name": "sum",
                        "sampling": {
                            "value": 10,
                            "unit": "minutes"
                        }
                    }]
                }]
			 }
         }
         ]
    }
'
