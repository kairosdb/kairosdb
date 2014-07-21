================
Grouping by Time
================

The time grouper groups results by time ranges. For example, you could group data by day of week.

*Note that the grouper calculates ranges based on the start time of the query.* So if you wanted to group by day of week and wanted the first group to be Sunday, then you need to set the query's start time to be on Sunday. 

------
Syntax
------

The name for this grouper is "time". 
::

  "name": "time"

The grouper takes a range size and a group count. The range is a value and a unit. For example, 1 day would group by day of the week (Sunday - Saturday). The group count is the number of groups. This would typically be 7 to group by day of week. But you could set this to 14 to group by fortnight. 
::

   "range_size": {
       "value": "the value",
       "unit": "the unit"
   },
   "group_count": "count"

-------
Example
-------
The example below groups data points by hours of the day for a week. This 
creates 168 groups (number of hours in a week). The first group is the first hour on Sunday, the second group is the second hour of Sunday, etc., until the last group which is the last hour on Saturday. Again this assumes that the query start time is set to Sunday at the first hour of the day.
::

  "group_by": [
        {
          "name": "time",
          "group_count": "168",
          "range_size": {
            "value": "1",
            "unit": "hours"
          }
        }
  ]

Each object of the response JSON contains the _group_by_ information you specified in the query as well as a _group_ object. The _group_ object contains the group number. In this example, the group number will be a number between 0 and 167 because there are 168 groups.
::

  {
    "queries": [
        {
            "results": [
                {
                   ...
                } ,
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "time",
                            "range_size": {
                                "value": 1,
                                "unit": "HOURS"
                            },
                            "group_count": 168,
                            "group": {
                                "group_number": 60
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": ["server1"]
                    },
                    "values": [
                        [1353222000000, 146],
                        [1353826800000, 241]
                    ]
                },
                {
                   ....
                },
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "time",
                            "range_size": {
                                "value": 1,
                                "unit": "HOURS"
                            },
                            "group_count": 168,
                            "group": {
                                "group_number": 156
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": ["server1"]
                    },
                    "values": [
                        [1353567600000, 2188],
                        [1354172400000, 3398],
                    ]
                }
            ]
        }
    ]
  }
