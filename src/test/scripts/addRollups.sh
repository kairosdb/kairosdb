#!/bin/bash

curl -s http://localhost:8080/api/v1/rollups -XPOST -H'Content-type:application/json' -d '
	 {
 	"name": "MyRollup",
 	"execution_interval": {
 		"value": 1,
 		"unit": "hours"
 	},
 	"rollups": [
 	{
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
 	},
 	 	{
 		"save_as": "kairosdb.datastore.query_collisions_rollup",
 		"query": {
 			"cache_time": 0,
 			"start_relative": {
 				"value": "1",
 				"unit": "hours"
 			},
 			"metrics": [{
 				"name": "kairosdb.datastore.query_collisions",
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
 	}
 	]
 }
'
