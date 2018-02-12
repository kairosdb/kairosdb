========
Features
========

The Features API returns metadata about various components of KairosDB. For example, this
API will return metadata about aggregators and GroupBys.

--------------------------------------------------------------------------------------------

Returns metadata for all features.

------
Method
------
  GET

-------
Request
-------

  http://[host]:[port]/api/v1/features

----
Body
----

  None

--------
Response
--------
*Success*
  Returns 200 when successful.
  ::
    [{
	"name": "groupby",
		"label": "Group By",
		"properties": [{
			"name": "tag",
			"label": "Tag",
			"description": "Groups data points by tag names.",
			"properties": [{
				"name": "tags",
				"label": "Tags",
				"description": "A list of tags to group by.",
				"optional": false,
				"type": "array",
				"options": [],
				"defaultValue": "[]",
				"autocomplete": "tags",
				"multiline": false,
				"validations": [{
					"expression": "value.length \u003e 0",
					"type": "js",
					"message": "Tags can\u0027t be empty."
				}]
			}]
		},
		...
	},
	{
		"name": "aggregators",
		"label": "Aggregator",
		"properties": [{
			"name": "avg",
			"label": "AVG",
			"description": "Averages the data points together.",
			"properties": [{
				"name": "align_sampling",
				"label": "Align sampling",
				"description": "When set to true the time for the aggregated data point for each range will fall on the start of the range instead of being the value for the first data point within that range. Note that align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.",
				"optional": false,
				"type": "boolean",
				"options": [],
				"defaultValue": "true",
				"autocomplete": "",
				"multiline": false,
				"validations": []
			}]
		},
		...
		}]
	}]

--------------------------------------------------------------------------------------------

Returns metadata for a particular feature.

------
Method
------
  GET

-------
Request
-------

  http://[host]:[port]/api/v1/features/{feature}

----
Body
----

  None

--------
Response
--------
*Success*
  Returns 200 when successful.
  ::

    {
		"name": "aggregators",
		"label": "Aggregator",
		"properties": [{
			"name": "avg",
			"label": "AVG",
			"description": "Averages the data points together.",
			"properties": [{
				"name": "align_sampling",
				"label": "Align sampling",
				"description": "When set to true the time for the aggregated data point for each range will fall on the start of the range instead of being the value for the first data point within that range. Note that align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.",
				"optional": false,
				"type": "boolean",
				"options": [],
				"defaultValue": "true",
				"autocomplete": "",
				"multiline": false,
				"validations": []
			}, {
				"name": "align_start_time",
				"label": "Align start time",
				"description": "Setting this to true will cause the aggregation range to be aligned based on the sampling size. For example if your sample size is either milliseconds, seconds, minutes or hours then the start of the range will always be at the top of the hour. The effect of setting this to true is that your data will take the same shape when graphed as you refresh the data. Note that align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.",
				"optional": false,
				"type": "boolean",
				"options": [],
				"defaultValue": "false",
				"autocomplete": "",
				"multiline": false,
				"validations": []
			}, {
        "name": "align_end_time",
        "label": "Align end time",
        "description": "Setting this to true will cause the aggregation range to be aligned based on the sampling size. For example if your sample size is either milliseconds, seconds, minutes or hours then the start of the range will always be at the top of the hour. The difference between align_start_time and align_end_time is that align_end_time sets the timestamp for the datapoint to the beginning of the following period versus the beginning of the current period. As with align_start_time, setting this to true will cause your data to take the same shape when graphed as you refresh the data. Note that align_start_time and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.",
        "optional": false,
        "type": "boolean",
        "options": [],
        "defaultValue": "false",
        "autocomplete": "",
        "multiline": false,
        "validations": []
      }, {
				"name": "sampling",
				"label": "Sampling",
				"optional": false,
				"type": "Object",
				"multiline": false,
				"properties": [{
					"name": "value",
					"label": "Value",
					"description": "The number of units for the aggregation buckets",
					"optional": false,
					"type": "long",
					"options": [],
					"defaultValue": "1",
					"autocomplete": "",
					"multiline": false,
					"validations": [{
						"expression": "value \u003e 0",
						"type": "js",
						"message": "Value must be greater than 0."
					}]
				}, {
					"name": "unit",
					"label": "Unit",
					"description": "The time unit for the sampling rate",
					"optional": false,
					"type": "enum",
					"options": ["MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS", "WEEKS", "MONTHS", "YEARS"],
					"defaultValue": "MILLISECONDS",
					"autocomplete": "",
					"multiline": false,
					"validations": []
				}]
			}]
		}
		...
	}]