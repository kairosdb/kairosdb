=================
List Aggregators
=================

Returns a list of all aggregators and their properties.

------
Method
------
  GET

-------
Request
-------

  http://[host]:[port]/api/v1/aggregators

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
    	"name": "diff",
    	"description": "Computes the difference between successive data points.",
    	"properties": []
    }, {
    	"name": "div",
    	"description": "Divides each data point by a divisor.",
    	"properties": [{
    		"name": "divisor",
    		"label": "divisor",
    		"description": "The value each data point is divided by.",
    		"optional": false,
    		"type": "double",
    		"options": [],
    		"defaultValue": "1",
    		"validation": "value > 0"
    	}]
    }, {
     	"name": "trim",
     	"description": "Trims off the first, last or both (first and last) data points from the results.",
     	"properties": [{
     		"name": "trim",
     		"label": "Trim",
     		"description": "Which data point to trim",
     		"optional": false,
     		"type": "enum",
     		"options": ["FIRST", "LAST", "BOTH"],
     		"defaultValue": "both",
     		"validation": ""
     	}]
     }]


