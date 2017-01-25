=============
List GroupBys
=============

Returns a list of all groupBys and their properties.

------
Method
------
  GET

-------
Request
-------

  http://[host]:[port]/api/v1/groupbys

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
		"name": "bin",
		"description": "Groups data points by bins or buckets.",
		"properties": [{
			"name": "bins",
			"label": "Bin Values",
			"description": "List of bin values. For example, if the list of bins is 10, 20, 30, then values less than 10 are placed in the first group, values between 10-19 into the second group, and so forth.",
			"optional": false,
			"type": "array",
			"options": [],
			"defaultValue": "[]",
			"validation": "value.length > 0"
		}]
	}, {
		"name": "tag",
		"description": "Groups data points by tag names.",
		"properties": [{
			"name": "tags",
			"label": "Tags",
			"description": "A list of tags to group by.",
			"optional": false,
			"type": "array",
			"options": [],
			"defaultValue": "[]",
			"validation": "value.length > 0"
		}]
	}, {
		"name": "time",
		"description": "Groups data points in time ranges.",
		"properties": [{
			"name": "groupCount",
			"label": "Count",
			"description": "The number of groups. This would typically be 7 to group by day of week.",
			"optional": false,
			"type": "int",
			"options": [],
			"defaultValue": "0",
			"validation": "value > 0"
		}, {
			"name": "rangeSize",
			"label": "Range Size",
			"optional": false,
			"type": "Object",
			"properties": [{
				"name": "value",
				"label": "Value",
				"description": "The number of units for the aggregation buckets",
				"optional": false,
				"type": "long",
				"options": [],
				"defaultValue": "1",
				"validation": "value > 0"
			}, {
				"name": "unit",
				"label": "Unit",
				"description": "The time unit for the sampling rate",
				"optional": false,
				"type": "enum",
				"options": ["MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS", "WEEKS", "MONTHS", "YEARS"],
				"defaultValue": "MILLISECONDS",
				"validation": ""
			}]
		}]
	}, {
		"name": "value",
		"description": "Groups data points by value.",
		"properties": [{
			"name": "rangeSize",
			"label": "Target Size",
			"description": "The range for each value. For example, if the range size is 10, then values between 0-9 are placed in the first group, values between 10-19 into the second group, and so forth.",
			"optional": false,
			"type": "int",
			"options": [],
			"defaultValue": "0",
			"validation": "value >= 0"
		}]
	}]

