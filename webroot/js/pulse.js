if (pulse === undefined) {
	var pulse = {};
}

pulse.MetricException = function (message) {
	this.message = message;
};

pulse.Aggregators =
{
	AVG: "avg",
	DEV: "dev",
	MAX: "max",
	MIN: "min",
	RATE: "rate",
	SORT: "sort",
	SUM: "sum"
};

pulse.Unit =  //Values used for Aggregator sampling and Relative time
{
	MILLISECONDS: "milliseconds",
	SECONDS: "seconds",
	MINUTES: "minutes",
	HOURS: "hours",
	DAYS: "days",
	WEEKS: "weeks",
	MONTHS: "months",
	YEARS: "years"
};

/**
 name: Name of the metric
 groupBy: tag to group results by
 */
pulse.Metric = function (name, groupBy) {
	this.tags = {};
	this.name = name;
	this.aggregators = [];

	if (groupBy && groupBy != undefined) {
		this.group_by = groupBy;
	}

	this.addTag = function (name, value) {
		this.tags[name] = value;
	};

	this.addAggregator = function(name, value, unit){
		var aggregator = {};
		aggregator.name = name;

		if (value && unit) {
			aggregator.sampling = {};
			aggregator.sampling.value = value;
			aggregator.sampling.unit = unit;
		}

		this.aggregators.push(aggregator);
	}
};

/**
 cacheTime: the amount of time in seconds to cache the query
 */
pulse.MetricQuery = function (cacheTime) {
	this.metrics = [];
	this.cache_time = 0;
	if (cacheTime != undefined)
		this.cache_time = cacheTime;

	/**
	 */
	this.setStartAbsolute = function (value) {
		this.start_absolute = value;
		if (this.start_relative != undefined)
			throw new pulse.MetricException(
				'You cannot define both start_absolute and start_relative');
	};

	/**
	 */
	this.setStartRelative = function (value, unit) {
		this.start_relative = {};
		this.start_relative.value = value;
		this.start_relative.unit = unit;
		if (this.start_absolute != undefined)
			throw new pulse.MetricException(
				'You cannot define both start_absolute and start_relative');
	};

	/**
	 */
	this.setEndAbsolute = function (value) {
		this.end_absolute = value;
		if (this.end_relative != undefined)
			throw new pulse.MetricException(
				'You cannot define both end_absolute and end_relative');
	};

	/**
	 */
	this.setEndRelative = function (value, unit) {
		this.end_relative = {};
		this.end_relative.value = value;
		this.end_relative.unit = unit;
		if (this.end_absolute != undefined)
			throw new pulse.MetricException(
				'You cannot define both end_absolute and end_relative');
	};

	/**
	 Used to add a pulse.GSMetric object to the MetricQuery
	 */
	this.addMetric = function (metric) {
		this.metrics.push(metric);
	};

	/**
	 Called to validate the MetricQuery object
	 */
	this.validate = function () {
		if ((this.start_relative == undefined) && (this.start_absolute == undefined))
			throw new pulse.MetricException(
				'You must define a start_relative or a start_absolute property');

		if (this.metrics.length == 0)
			throw new pulse.MetricException(
				'You must specify one or more metrics to query upon');
	}
};

pulse.showError = function (message) {
	alert(message);
};

//---------------------------------------------------------------------------
/**
 @param values Array of arrays of timestamp, value
 @returns: Array[0-6] each containing an array of values for that day
 */
pulse.collectWeeklyValues = function (values) {
	var week = [];

	$.each(values, function (i, val) {
		var date = new Date(0);
		date.setUTCSeconds(val[0]);
		var day = date.getDay();
		if (week[day] == undefined)
			week[day] = [];

		week[day].push(val[1]);
	});

	return (week);
};


//---------------------------------------------------------------------------
/**
 Takes the return from collectWeeklyValues and averages them to one number
 @return: returns an array of numbers for each day of the week
 */
pulse.averageWeeklyValues = function (weekData) {
	var weekAvg = [];

	for (i = 0; i < 7; i++) {
		if (weekData[i] == undefined) {
			weekAvg[i] = 0;
		}
		else {
			var dayValues = weekData[i];
			var sum = 0;
			dayValues.forEach(function (element) {
				sum += element;
			});
			weekAvg[i] = sum / dayValues.length;
		}
	}

	return weekAvg;
};

