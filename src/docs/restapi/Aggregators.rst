===========
Aggregators
===========

.. highlight:: json

.. note::
	Please ignore the () on the aggregators below, it is a side effect of the documentation.

----------------
Range Aggregator
----------------

Many of the below aggregators inherit from the range aggregator.  You can set
the following parameters on any range aggregator.

**sampling** (Sampling {value (long), unit (MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS, WEEKS, MONTHS, YEARS)})

	Sampling is the length of the interval on which to aggregate data.

	.. code-block:: json
		"aggregators": [
        {
			"name": "sum",
			"align_sampling": true,
			"align_start_time": true,
			"sampling": {
				"value": "1",
				"unit": "minutes"
			}
        }]

**align_start_time** - (boolean)

	When set to true the time for the aggregated data point for each range will
	fall on the start of the range instead of being the value for the first
	data point within that range.

**align_sampling** - (boolean)

	Setting this to true will cause the aggregation range to be aligned based on
	the sampling size.  For example if your sample size is either milliseconds,
	seconds, minutes or hours then the start of the range will always be at the top
	of the hour.  The effect of setting this to true is that your data will
	take the same shape when graphed as you refresh the data.

**start_time** - (long)

	Start time to calculate the ranges from.  Typically this is the start of the query

-------
Average
-------
.. function:: avg

	Computes average value.

	:param align_start_time:

------------------
Standard Deviation
------------------
.. function:: dev

	Computes standard deviation.

-----
Count
-----
.. function:: count

	Counts the number of data points.

-----
First
-----
.. function:: first

	Returns the first data point for the interval.


----
Gaps
----
.. function:: gaps

	Marks gaps in data according to sampling rate with a null data point.

---------
Histogram
---------
.. function:: histogram

	Calculates a probability distribution and returns the specified percentile
	for the distribution. The "percentile" value is defined as 0 < percentile <= 1
	where .5 is 50% and 1 is 100%. Note that this aggregator has been renamed to
	*percentile* in release 0.9.2.

----
Last
----
.. function:: last

	Returns the last data point for the interval.

-------------
Least Squares
-------------
.. function:: least_squares

	Returns two points for the range which represent the best fit line through the set of points.

----
Max
----
.. function:: max

	Returns the largest value in the interval.

----
Min
----
.. function:: min

	Returns the smallest value in the interval.

----------
Percentile
----------
.. function:: percentile

	Finds the percentile of the data range. Calculates a probability distribution
	and returns the specified percentile for the distribution. The “percentile”
	value is defined as 0 < percentile <= 1 where .5 is 50% and 1 is 100%.

----
Sum
----
.. function:: sum

	Sums all values

----
Diff
----
.. function:: diff

	Computes the difference between successive data points.

------
Divide
------
.. function:: div

	Returns each data point divided by a divisor. Requires a "divisor" property
	which is the value that all data points will be divided by.

----
Rate
----
.. function:: rate

	Returns the rate of change between a pair of data points. Requires a "unit"
	property which is the sampling duration (ie rate in seconds, milliseconds,
	minutes, etc...).

-------
Sampler
-------
.. function:: sampler

	Computes the sampling rate of change for the data points. Requires a "unit"
	property which is the sampling duration  (ie rate in seconds, milliseconds,
	minutes, etc...).

-----
Scale
-----
.. function:: scale

	Scales each data point by a factor. Requires a "factor" property which is
	the scaling value.

----
Trim
----
.. function:: trim

	Trims off the first, last or both data points for the interval.  Useful in
	conjunction with the save_as aggregator to remove partial intervals.

-------
Save As
-------
.. function:: save_as

	Saves the result to another metric.

	:param ttl: Time to live on the saved data