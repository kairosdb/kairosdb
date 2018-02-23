========
Roll-ups
========

Roll-ups are a way to improve query performance by aggregating or rolling up data into a larger time range such as averaging millisecond data over a minute.
Roll-ups perform queries on existing data, aggregate the data, and write the results to another metric. The original data is left intact.
Roll-ups are continuously run on a schedule rolling up a small time period of data on each execution.
Roll-ups are scheduled by creating a roll-up task. A task contains one or more roll-ups and an execution interval (how often the task is executed).
Roll-ups can be created using the :doc:`Roll-up REST API <restapi/Roll-ups>` or by using the Web UI (`http://<kairosServer>:<port>/rollups.html`).

----------
Installing
----------
Roll-ups are installed with KairosDB but disabled by default. You enable roll-ups by uncommenting the following line from kairosdb.properties and restarting KairosDB:

::

	kairosdb.service.rollups=org.kairosdb.rollup.RollUpModule

-------
Example
-------

A common use case for Roll-ups is monitoring systems with dashboards. We monitor the health of our systems by viewing dashboards that span several days or weeks
worth of data. The dashboards update regularly and we want them to update quickly when the data is refreshed. But if a dashboard shows an anomaly we
want the option to drill down into more granular data. So data is reported at millisecond precision, but dashboards are created by roll-up tasks that
roll-up the data on hourly, daily, or weekly precision. Roll-ups improve the performance of the queries because fewer data points are required to plot a graph
of daily values versus aggregating millisecond data.

We measure, for example, the total number of a particular type of event that enters our system and track that over a 3 month period. The data is measured in milliseconds.
The results of a query for 3 months and aggregated over a week are:

::

	Query Time: 24 seconds
	Data Points Returned: 1,535,711
	Data Points Plotted: 12


If however, we create a roll-up task that rolls up to daily values, the query is significantly faster:

::

    Query Time: 417 milliseconds
    Data Points Returned: 2,045
    Data Points Plotted: 12


