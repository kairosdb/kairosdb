#!/bin/bash

curl -s http://localhost:8080/api/v1/rollups/rollup -XPOST -H'Content-type:application/json' -d '
	[
		{
			"metric_name": "test_metric",
			"start_relative":
			{
				"value": 5,
				"unit": "minutes"
			},
			"end_relative":
			{
				"value": 10,
				"unit": "years"
			},
			"schedule": "0 */10 * * * ?"
		}
	]
'
