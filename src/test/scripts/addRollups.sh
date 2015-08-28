#!/bin/bash

curl -s http://localhost:8080/api/v1/rollups/rollup -XPOST -H'Content-type:application/json' -d '
	{
		"name": "monthly-rollup",
        "schedule": "* * * * * ?",
        "rollups": [
         {
	         "save_as": "metric2",
	         "tags": {},
             "query":
             {
			   "start_relative": {
                    "value": "5",
                    "unit": "minutes"
                },
			   "metrics": [
                {
                    "tags": {
                        "host": ["foo", "foo2"],
                        "customer": ["bar"]
                    },
                    "name": "abc.123",
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
