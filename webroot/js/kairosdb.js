if (kairosdb === undefined) {
	var kairosdb = {};
}

kairosdb.MetricException = function (message) {
	this.message = message;
};

kairosdb.Aggregators =
{
	AVG: "avg",
	DEV: "dev",
	MAX: "max",
	MIN: "min",
	RATE: "rate",
	SAMPLER: "sampler",
	SORT: "sort",
	SUM: "sum",
	LEAST_SQUARES: "least_squares",
	PERCENTILE: "percentile",
	SCALE: "scale",
	GAPS: "gaps",
	FIRST: "first",
	LAST: "last",
	FILTER: "filter",
	TRIM: "trim",
	SAVE_AS: "save_as"
};

kairosdb.Unit =  //Values used for Aggregator sampling and Relative time
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
 */
kairosdb.Metric = function (name) {
	this.tags = {};
	this.name = name;
	this.aggregators;
	this.group_by;
	this.limit;

	this.addGroupBy = function (groupBy) {
		if (!this.group_by) {
			this.group_by = [];
		}

		this.group_by.push(groupBy);
		return this;
	};

	this.addTag = function (name, value) {
		if (!this.tags[name]) {
			this.tags[name] = [];
		}

		this.tags[name].push(value);
		return this;
	};

	this.addRate = function (unit, time_zone) {
		if (!this.aggregators)
			this.aggregators = [];

		var rate = {};
		rate.name = "rate";
		if (unit) {
			rate.sampling = {};
			rate.sampling.unit = unit;
			rate.sampling.value = 1;
			rate.sampling.time_zone = time_zone;
		}

		this.aggregators.push(rate);
		return this;
	};

	this.addSampler = function (unit) {
		if (!this.aggregators)
			this.aggregators = [];

		var rate = {};
		rate.name = "sampler";
		if (unit) {
			rate.unit = unit;
		}

		this.aggregators.push(rate);
		return this;
	};

	this.addPercentile = function (value, unit, percent, time_zone, align) {
		if (!this.aggregators)
			this.aggregators = [];

		var percentile = this.addRangeAggregator("percentile", value, unit, time_zone, align);
		percentile.percentile = percent;

		return this;
	};

	this.addDivideAggregator = function (divisor) {
		if (!this.aggregators)
			this.aggregators = [];

		var aggregator = {};
		aggregator.name = "div";
		if (divisor) {
			aggregator.divisor= divisor;
		}

		this.aggregators.push(aggregator);
		return this;
	};

	this.addFilterAggregator = function (filterop, threshold) {
		if (!this.aggregators)
			this.aggregators = [];

		var aggregator = {};
		aggregator.name = "filter";
		aggregator.filter_op = filterop;
		if (threshold) {
			aggregator.threshold = threshold;
		}

		this.aggregators.push(aggregator);
		return this;
	};

	/**
	 Genereic add Aggregator function, returns the new aggregator
	 * @param name
	 * @returns {{}}
	 */
	this.addAggregator = function (name) {
		if (!this.aggregators)
			this.aggregators = [];

		var aggregator = {};
		aggregator.name = name;

		this.aggregators.push(aggregator);
		return aggregator;
	};

	this.addRangeAggregator = function (name, value, unit, time_zone, align) {
		if (!this.aggregators)
			this.aggregators = [];

		var aggregator = {};
		aggregator.name = name;

		if (value && unit) {
			aggregator.sampling = {};
			aggregator.sampling.value = value;
			aggregator.sampling.unit = unit;
			aggregator.sampling.time_zone = time_zone;
			if (align == "sample") aggregator.align_sampling = true;
			if (align == "start") aggregator.align_start_time = true;
			if (align == "end") aggregator.align_end_time = true;
		}

		this.aggregators.push(aggregator);
		return aggregator;
	};

	this.addScaleAggregator = function (scalingFactor) {
		if (!this.aggregators)
			this.aggregators = [];

		var aggregator = {};
		aggregator.name = 'scale';
		if (scalingFactor) {
            aggregator.factor = scalingFactor;
		}

        this.aggregators.push(aggregator);

	    return this;
	}

    this.setLimit = function (value) {

        this.limit = value;
        return this;
    };
};

/**
 * Tag groupBy
 * @param tags  space or comma delimited list of tag names
 */
kairosdb.TagGroupBy = function (tags) {
	this.name = "tag";
	this.tags = [];

	if (tags) {
		this.tags = tags.trim().split(/[\s,]+/);
	}
};

/**
 * Value groupBy
 * @param groupSize
 */
kairosdb.ValueGroupBy = function (groupSize) {
	this.name = "value";
	this.range_size = groupSize;
};

/**
 * Time groupBy
 * @param groupSizeValue group size value
 * @param groupSizeUnit group size unit: milliseconds, seconds, minutes, hours, days, months, years
 * @param groupCount group count
 */
kairosdb.TimeGroupBy = function (groupSizeValue, groupSizeUnit, groupCount) {
	this.name = "time";
	this.group_count = groupCount;
	this.range_size = {};

	this.range_size.value = groupSizeValue;
	this.range_size.unit = groupSizeUnit;
};

/**
 * Bin groupBy
 * @param groupSize
 */
kairosdb.BinGroupBy = function (groupSize) {
	this.name = "bin";
	this.bins = groupSize;
}
/**
 cacheTime: the amount of time in seconds to cache the query
 */
kairosdb.MetricQuery = function (cacheTime) {
	this.metrics = [];
	this.plugins = [];
	this.cache_time = 0;
	if (cacheTime != undefined)
		this.cache_time = cacheTime;

	/**
	 */
	this.setStartAbsolute = function (value) {
		this.start_absolute = value;
		if (this.start_relative != undefined)
			throw new kairosdb.MetricException(
				'You cannot define both start_absolute and start_relative');
	};

	/**
	 */
	this.setStartRelative = function (value, unit) {
		this.start_relative = {};
		this.start_relative.value = value;
		this.start_relative.unit = unit;
		if (this.start_absolute != undefined)
			throw new kairosdb.MetricException(
				'You cannot define both start_absolute and start_relative');
	};

	/**
	 */
	this.setEndAbsolute = function (value) {
		this.end_absolute = value;
		if (this.end_relative != undefined)
			throw new kairosdb.MetricException(
				'You cannot define both end_absolute and end_relative');
	};

	/**
	 */
	this.setEndRelative = function (value, unit) {
		this.end_relative = {};
		this.end_relative.value = value;
		this.end_relative.unit = unit;
		if (this.end_absolute != undefined)
			throw new kairosdb.MetricException(
				'You cannot define both end_absolute and end_relative');
	};

	/**
	 */
	this.setTimeZone = function (timeZone) {
		this.time_zone = timeZone;
	}

	/**

	 */
	/*this.setPostScript = function (postScript) {
		var plugin = {};
		plugin.name = 'kairos_script';
		plugin.script = postScript;
		this.plugins.push(plugin);
	}*/

	/**
	 Used to add a kairos.Metric object to the MetricQuery
	 */
	this.addMetric = function (metric) {
		this.metrics.push(metric);
	};

	/**
	 Called to validate the MetricQuery object
	 */
	this.validate = function () {
		if ((this.start_relative == undefined) && (this.start_absolute == undefined))
			throw new kairosdb.MetricException(
				'You must define a start_relative or a start_absolute property');

		if (this.metrics.length == 0)
			throw new kairosdb.MetricException(
				'You must specify one or more metrics to query upon');
	}
};

kairosdb.showError = function (message) {
	alert(message);
};

//---------------------------------------------------------------------------
/**
 @param values Array of arrays of timestamp, value
 @returns: Array[0-6] each containing an array of values for that day
 */
kairosdb.collectWeeklyValues = function (values) {
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
kairosdb.averageWeeklyValues = function (weekData) {
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

