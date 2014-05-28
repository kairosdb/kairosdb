=================
Grouping by Value
=================

The value grouper groups by data point values. Values are placed into groups based on a range size. For example, if the range size is 10, then values between 0-9 are placed in the first group, values between 10-19 into the second group, and so forth.

------
Syntax
------

The name for this grouper is "value".
::

    "name": "value"

The grouper requires a range size. This is range of the values for each group.
::

    "range*size": size

-------
Example
-------

This example groups value by a range size of 1000.
::

  "group_by": [
        {
          "name": "value",
          "range_size": 1000
        }
  ]

Each object of the response JSON contains the *group_by* information you specified in the query as well as a *group* object. The *group* object contains the group number starting with a group number of 0. For example,
the first group (group number 0) contains data points whose values are between 0 and 999. The second group (group number 1) contains data points whose values are between 1000 and 1999, etc.
::

  {
    "queries": [
        {
            "results": [
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "value",
                            "range_size": 1000,
                            "group": {
                                "group_number": 0
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": [server1"]
                    },
                    "values": [
                        [1353222000000, 146],
                        [1353567600000, 697]
                    ]
                },
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "value",
                            "range_size": 1000,
                            "group": {
                                "group_number": 1
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": ["server2"]
                    },
                    "values": [
                        [1353567600000, 1491],
                        [1353913200000, 2978],
                        [1353999600000, 2592]
                    ]
                }
            ]
        }
    ]
  }
