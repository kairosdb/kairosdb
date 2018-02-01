===========
Aggregators
===========

---------------------
Aggregator Parameters
---------------------

The following are parameters that are common to more than one aggregator.

.. _unit:

.. js:data:: unit

	Unit is a time unit represented as a string and must be one of (
	MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS, WEEKS, MONTHS, YEARS)

.. _sampling:

.. js:data:: sampling

	A sampling is a json object containing two values a ``value`` and a ``unit``.
	The value is a long and the unit is see :ref:`unit`.


.. _range_aggregator:

----------------
Range Aggregator
----------------

Many of the below aggregators inherit from the range aggregator.  You can set
the following parameters on any range aggregator.

**sampling** (Sampling {value (long), unit (See :ref:`unit`)})

	Sampling is the length of the interval on which to aggregate data.

.. code-block:: javascript

	"aggregators": [
	{
	  "name": "sum",
	  "align_sampling": true,
	  "align_start_time": true,
	  "align_end_time": false,
	  "sampling": {
	    "value": 1,
	    "unit": "minutes"
	  }
	}]

**align_start_time** - (boolean, optional, default value: false)

	When set to true the time for the aggregated data point for each range will
	fall on the start of the range instead of being the value for the first
	data point within that range. Note that align_sampling, align_start_time, and align_end_time
  are mutually exclusive. If more than one are set, unexpected results will occur.

**align_end_time** - (boolean, optional, default value: false)

  Setting this to true will cause the aggregation range to be aligned based on the sampling
  size. For example if your sample size is either milliseconds, seconds, minutes or hours then the
  start of the range will always be at the top of the hour. The difference between align_start_time
  and align_end_time is that align_end_time sets the timestamp for the datapoint to the beginning of
  the following period versus the beginning of the current period. As with align_start_time, setting
  this to true will cause your data to take the same shape when graphed as you refresh the data. Note
  that align_start_time and align_end_time are mutually exclusive. If more than one are set, unexpected
  results will occur.

**align_sampling** - (boolean, optional, default value: false)

	Setting this to true will cause the aggregation range to be aligned based on
	the sampling size.  For example if your sample size is either milliseconds,
	seconds, minutes or hours then the start of the range will always be at the top
	of the hour.  The effect of setting this to true is that your data will
	take the same shape when graphed as you refresh the data. Note that 
  align_sampling, align_start_time, and align_end_time are mutually exclusive.
  If more than one are set, unexpected results will occur.

**start_time** - (long, optional, default value: 0)

	Start time to calculate the ranges from.  Typically this is the start of the query

**time_zone** - (long time zone format)

	Time zone to use when doing time based calculations.

-------
Average
-------
.. js:data:: avg

	Computes average value.
	Extends :ref:`range_aggregator`.

------------------
Standard Deviation
------------------
.. js:data:: dev

	Computes standard deviation.
	Extends :ref:`range_aggregator`.

-----
Count
-----
.. js:data:: count

	Counts the number of data points.
	Extends :ref:`range_aggregator`.

-----
First
-----
.. js:data:: first

	Returns the first data point for the interval.
	Extends :ref:`range_aggregator`.

----
Gaps
----
.. js:data:: gaps

	Marks gaps in data according to sampling rate with a null data point.
	Extends :ref:`range_aggregator`.

---------
Histogram
---------
.. js:data:: histogram

	Calculates a probability distribution and returns the specified percentile
	for the distribution. The "percentile" value is defined as 0 < percentile <= 1
	where .5 is 50% and 1 is 100%. Note that this aggregator has been renamed to
	*percentile* in release 0.9.2.
	See :ref:`percentile_aggregator`.

----
Last
----
.. js:data:: last

	Returns the last data point for the interval.
	Extends :ref:`range_aggregator`.

-------------
Least Squares
-------------
.. js:data:: least_squares

	Returns two points for the range which represent the best fit line through the set of points.
	Extends :ref:`range_aggregator`.

----
Max
----
.. js:data:: max

	Returns the largest value in the interval.
	Extends :ref:`range_aggregator`.

----
Min
----
.. js:data:: min

	Returns the smallest value in the interval.
	Extends :ref:`range_aggregator`.

.. _percentile_aggregator:

----------
Percentile
----------
.. js:data:: percentile

	Finds the percentile of the data range. Calculates a probability distribution
	and returns the specified percentile for the distribution. The “percentile”
	value is defined as 0 < percentile <= 1 where .5 is 50% and 1 is 100%.
	Extends :ref:`range_aggregator`.

	Parameters:
		**percentile** (double) - Percentile to count.

----
Sum
----
.. js:data:: sum

	Sums all values
	Extends :ref:`range_aggregator`.

----
Diff
----
.. js:data:: diff

	Computes the difference between successive data points.

------
Divide
------
.. js:data:: div

	Returns each data point divided by a divisor. Requires a "divisor" property
	which is the value that all data points will be divided by.

	Parameters:
		**divisor** (double) - Value to divide data points by.

----
Rate
----
.. js:data:: rate

	Returns the rate of change between a pair of data points. Requires a "unit"
	property which is the sampling duration (ie rate in seconds, milliseconds,
	minutes, etc...).

	Parameters:
		**sampling** (See :ref:`sampling`) - Sets the sampling for calculating
		the rate.

		**unit** (See :ref:`unit`) - Shortcut for setting the sampling to a single unit.
		If you set the unit to ``SECONDS`` then the sampling is over one second.

		**time_zone** (Long format time zone) - Time zone for doing time calculations.

-------
Sampler
-------
.. js:data:: sampler

	Computes the sampling rate of change for the data points. Requires a "unit"
	property which is the sampling duration  (ie rate in seconds, milliseconds,
	minutes, etc...).

	Parameters:
		**unit** (See :ref:`unit`) - Sets the sampling unit.
		If you set the unit to ``SECONDS`` then the sampling rate is over one second.

		**time_zone** (Long format time zone) - Time zone for doing time calculations.

-----
Scale
-----
.. js:data:: scale

	Scales each data point by a factor. Requires a "factor" property which is
	the scaling value.

	Parameters:
		**factor** (double) - Scale factor.

----
Trim
----
.. js:data:: trim

	Trims off the first, last or both data points for the interval.  Useful in
	conjunction with the save_as aggregator to remove partial intervals.

	Parameters:
		**trim** (FIRST, LAST, BOTH) - Trims either first, last or both end data points.

-------
Save As
-------
.. js:data:: save_as

	Saves the result to another metric.  Any data point with a unique tag value will also
	have that tag set.  So if a data point is returned with tags ``{"dc":["DC1"],"host":["hostA", "hostB"]}``
	only the dc tag will be set when saved.  If you do a group by query the group by tags are saved.

	Parameters:
		**metric_name** (string) - Metric name to save the results to.

		**tags** (Map of key values) - Additional tags to set on the metrics ``{"tag1":"value1","tag2":"value2"}``

		**ttl** (integer) - Sets the ttl on the newly saved metrics

		**add_saved_from** (boolean) - Tells the aggregator to add the saved_from tag to the new metric.  Defaults to true.

------
Filter
------
.. js:data:: filter

	Filters out data points matching given critera.

	Parameters:
		**filter_op** (LTE, LT, GTE, GT, EQUAL) - Defines what data points to filter in relation to the threshold.

		**threshold** (double) - Sets the threshold value for filtering data points.

-------------
JS Aggregator
-------------
.. js:data:: js_function
.. js:data:: js_filter
.. js:data:: js_range

	The JS Aggregator is provided as a thrid party module found here

	https://github.com/Kratos-ISE/kise-kairosdb-module/

	The module requires Java 8 and provides a way to pass javascript code as the
	aggregator.