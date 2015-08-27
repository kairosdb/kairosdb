=================
Grouping by Value
=================

The Bin grouper groups data point values into bins or buckets. Values are placed into groups based on a list of bin values. For example, if the list of bins is 10, 20, 30, then values less than 10 are placed in the first group, values between 10-19 into the second group, and so forth.

------
Syntax
------

The name for this grouper is "bin".
::

    "name": "bin"

The grouper requires a bins property. This is a list of bin values.
::

    "bins": [bin1, bin2, bin3, ...]

-------
Example
-------

This example groups values into groups of 2.
::

  "group_by": [
        {
          "name": "bin",
          "bins": ["2", "4", "6", "8"]
        }
  ]

Each object of the response JSON contains the *group_by* information you specified in the query as well as a *group* object. The *group* object contains the group number starting with a group number of 0. For example,
the first group (bin number 0) contains data points whose values are between 0 and 2. The second group (bin number 1) contains data points whose values are between 2 and 4, etc.
::

  {
    "queries": [
        {
            "results": [
                {
                    "name": "metric1",
                    "group_by": [
                     {
                        "name": "bin",
                        "bins": ["2", "4", " 6", " 8"],
                         "group": {
                            "bin_number": 0
                         }
                    }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": [server1"]
                    },
                    "values": [
                        [1353222000000, 1],
                        [1353567600000, 1]
                    ]
                },
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "bin",
                            "bins": ["2", "4", " 6", " 8"],
                            "group": {
                                "bin_number": 1
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": ["server2"]
                    },
                    "values": [
                        [1353567600000, 2],
                        [1353913200000, 2],
                        [1353999600000, 3]
                    ]
                },
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "bin",
                            "bins": ["2", "4", " 6", " 8"],
                            "group": {
                                "bin_number": 2
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": ["server2"]
                    },
                    "values": [
                        [1353567600000, 4],
                    ]
                },
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "bin",
                            "bins": ["2", "4", " 6", " 8"],
                            "group": {
                                "bin_number": 3
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": ["server2"]
                    },
                    "values": [
                        [1353567600000, 6],
                    ]
                }
            ]
        }
    ]
  }
