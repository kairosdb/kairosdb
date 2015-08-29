#!/bin/bash

curl -s http://localhost:8080/api/v1/rollups/rollup -XPOST -H'Content-type:application/json' -d '
	{
		"name": "monthly-rollup",
        "schedule": "0 * * * * ?",
        "rollups": [
         {
	         "save_as": "rolluptest1",
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
                    "name": "kairosdb.http.query_time",
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
         }]
    }'
