================
Grouping by Tags
================
You can group results by specifying one or more tag names. For example, if you have a *customer* tag, grouping by *customer* would create a resulting object for each customer.

Multiple tag names can be used to further group the data points.

------
Syntax
------

The name for this grouper is "tag". 
::

    "name": "tag"

The grouper takes an array of tag names.
::

    "tags": ["tagName1", "tagName2"]

-------
Example
-------

The example below groups by both *data_center* and *host*. If, for example, there were two data centers *dc1* and *dc2* and two hosts *server1* and *server2*, you could group by the combination of *host* and *data_center* by listing both tag names in the *tags* property.

The response JSON would contain four objects, one for each combination of data center and host (assuming there were data points for all four combinations).
::

  "group_by": [
          {
            "name": "tag",
            "tags": ["data_center", "host"]
          }
        ]

Each object of the response JSON contains the *group_by* information you specified in the query as well as a *group* object. The *group* object contains the tags names and their corresponding values for the particular  grouping. The first group in the results below include data points for the *dc1* data center and *server1* host.
::

  {
    "queries": [
        {
            "results": [
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "tag",
                            "tags": ["data_center", "host"],
                            "group": {
                                "data_center": "dc1",
                                "host": "server1"
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc1"],
                        "host": ["server1"]
                    },
                    "values": [
                        [1353222000000, 31],
                        [1364796000000, 723]
                    ]
                },
                {
                    "name": "metric1",
                    "group_by": [
                        {
                            "name": "tag",
                            "tags": ["data_center", "host"],
                            "group": {
                                "data_center": "dc2",
                                "host": "server1"
                            }
                        }
                    ],
                    "tags": {
                        "data_center": ["dc2"],
                        "host": ["server1"]
                    },
                    "values": [
                        [1353222000000, 108],
                        [1364796000000, 1318]
                    ]
                },
              
               ...

            ]
        }
    ]
  }
