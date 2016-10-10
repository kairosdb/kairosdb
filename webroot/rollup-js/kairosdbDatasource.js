var tagList = null;

module.factory('KairosDBDatasource', function ($q, $http) {

	function KairosDBDatasource() {
	}

	KairosDBDatasource.query = function (options) {
		var start = options.range.from;
		var end = options.range.to;
		var queries = _.compact(_.map(options.targets, _.partial(convertTargetToQuery, options)));
		// No valid targets, return the empty result to save a round trip.
		if (_.isEmpty(queries)) {
			var d = $q.defer();
			d.resolve({data: []});
			return d.promise;
		}
		return this.performTimeSeriesQuery(queries, start, end).then(handleKairosDBQueryResponse, handleQueryError);
	};

	///////////////////////////////////////////////////////////////////////
	/// Query methods
	///////////////////////////////////////////////////////////////////////

	KairosDBDatasource.performTimeSeriesQuery = function (queries, start, end) {
		var reqBody = {
			metrics: queries
		};
		reqBody.cache_time = 0;
		convertToKairosTime(start, reqBody, 'start');
		convertToKairosTime(end, reqBody, 'end');
		var options = {
			method: 'POST',
			url: '/api/v1/datapoints/query',
			data: reqBody
		};

		return $http(options);
	};

	/**
	 * Gets the list of metrics
	 * @returns {*|Promise}
	 */
	KairosDBDatasource.performMetricSuggestQuery = function () {
		var options = {
			url: '/api/v1/metricnames',
			method: 'GET'
		};
		return $http(options).then(function (results) {
			if (!results.data) {
				return [];
			}
			return results.data.results;
		});
	};

	KairosDBDatasource.performTagSuggestQuery = function (metricname, type, keyValue) {
		if (!metricname || _.isEmpty(metricname)) {
			return;
		}

		if (tagList && tagList.results && (metricname === tagList.metricName)) /*&& (range.from === tagList.range.from) &&	(range.to === tagList.range.to))*/
		{
			return getTagListFromResponse(tagList.results, type, keyValue);
		}

		tagList = {
			metricName: metricname
		};
		var body = {
			metrics: [{name: metricname}],
			start_absolute: 0

		};
		//convertToKairosTime(range.from, body, 'start');
		//convertToKairosTime(range.to, body, 'end');
		var options = {
			url: '/api/v1/datapoints/query/tags',
			method: 'POST',
			data: body
		};
		return $http(options).then(function (results) {
				tagList.results = results;
				return getTagListFromResponse(results, type, keyValue);
			},
			function errorCallback(response) {
				console.log(response);  // todo do something better
			});

	};

	/////////////////////////////////////////////////////////////////////////
	/// Formatting methods
	////////////////////////////////////////////////////////////////////////

	function getTagListFromResponse(results, type, keyValue) {
		if (!results.data) {
			return [];
		}
		if (type === "key") {
			return _.keys(results.data.queries[0].results[0].tags);
		}
		else if (type === "value" && _.has(results.data.queries[0].results[0].tags, keyValue)) {
			return results.data.queries[0].results[0].tags[keyValue];
		}
		return [];
	}

	/**
	 * Requires a verion of KairosDB with every CORS defects fixed
	 * @param results
	 * @returns {*}
	 */
	function handleQueryError(results) {
		if (results.data.errors && !_.isEmpty(results.data.errors)) {
			var errors = {
				message: results.data.errors[0]
			};
			return $q.reject(errors);
		}
		else {
			return $q.reject(results);
		}
	}

	function handleKairosDBQueryResponse(results) {
		var output = [];
		_.each(results.data.queries, function (series) {
			//var sample_size = series.sample_size;
			//console.log("sample_size:" + sample_size + " samples");

			_.each(series.results, function (result) {

				var target = result.name;
				if (result.group_by) {
					target += " ( ";
					_.each(result.group_by, function (element) {
						if (element.name === "tag") {
							_.each(element.group, function (value, key) {
								target += key + "=" + value + " ";
							});
						}
						else if (element.name === "value") {
							target += 'value_group=' + element.group.group_number + " ";
						}
						else if (element.name === "time") {
							target += 'time_group=' + element.group.group_number + " ";
						}
					});
					target += ") ";
				}
				var datapoints = [];

				for (var i = 0; i < result.values.length; i++) {
					var t = result.values[i][0];
					var v = result.values[i][1];
					datapoints[i] = [v, t];
				}
				output.push({target: target, datapoints: datapoints});
			});
		});
		return {data: _.flatten(output)};
	}

	KairosDBDatasource.convertTargetToQuery = function (options, target) {
		if (!target.metric || target.hide) {
			return null;
		}

		var query = {
			name: target.metric
		};

		query.aggregators = [];
		if (target.downsampling !== '(NONE)') {
			query.aggregators.push({
				name: target.downsampling,
				align_sampling: true,
				sampling: KairosDBDatasource.convertToKairosInterval(target.sampling || options.interval)
			});
			if (target.samplingAlignStartTime) {
				query.aggregators[0].align_start_time = true;
			}
		}
		if (target.horizontalAggregators) {
			_.each(target.horizontalAggregators, function (chosenAggregator) {
				var returnedAggregator = {
					name: chosenAggregator.name
				};
				if (chosenAggregator.sampling_rate) {
					returnedAggregator.sampling = KairosDBDatasource.convertToKairosInterval(chosenAggregator.sampling_rate);
					returnedAggregator.align_sampling = true;
					if (chosenAggregator.align_start_time) {
						returnedAggregator.align_start_time = true;
					}
				}
				if (chosenAggregator.unit) {
					returnedAggregator.unit = chosenAggregator.unit + 's';
				}
				if (chosenAggregator.factor && chosenAggregator.name === 'div') {
					returnedAggregator.divisor = chosenAggregator.factor;
				}
				else if (chosenAggregator.factor && chosenAggregator.name === 'scale') {
					returnedAggregator.factor = chosenAggregator.factor;
				}
				if (chosenAggregator.percentile) {
					returnedAggregator.percentile = chosenAggregator.percentile;
				}
				query.aggregators.push(returnedAggregator);
			});
		}
		if (_.isEmpty(query.aggregators)) {
			delete query.aggregators;
		}

		if (target.tags) {
			query.tags = angular.copy(target.tags);
		}

		if (target.groupByTags || target.nonTagGroupBys) {
			query.group_by = [];
			if (target.groupByTags) {
				query.group_by.push({
					name: "tag",
					tags: angular.copy(target.groupByTags)
				});
			}
			if (target.nonTagGroupBys) {
				_.each(target.nonTagGroupBys, function (rawGroupBy) {
					var formattedGroupBy = angular.copy(rawGroupBy);
					if (formattedGroupBy.name === 'time') {
						formattedGroupBy.range_size = KairosDBDatasource.convertToKairosInterval(formattedGroupBy.range_size);
					}
					query.group_by.push(formattedGroupBy);
				});
			}
		}
		return query;
	};

	///////////////////////////////////////////////////////////////////////
	/// Time conversion functions specifics to KairosDB
	//////////////////////////////////////////////////////////////////////

	/// "10 seconds" -> {value: 10, unit: "seconds"
	KairosDBDatasource.convertLongFormatToKairosUnit = function (time) {
		// todo add validation
		var timeArray = time.split(" ");
		var unit = endsWith(timeArray[1], "s") ? timeArray[1] : timeArray[1] + "s";

		return {
			"value": timeArray[0],
			"unit": unit
		}
	};

	/// "10 seconds" -> {value: 10, unit: "seconds"
	KairosDBDatasource.convertKairosUnitToLongFormat = function (timeUnit) {
		// todo add validation
		if (timeUnit.value == 1)
			return timeUnit.value + " " + timeUnit.unit.substring(0, timeUnit.unit.length - 1);
		else
			return timeUnit.value + " " + timeUnit.unit;
	};

	KairosDBDatasource.convertToKairosInterval = function (intervalString) {
		var interval_regex = /(\d+(?:\.\d+)?)([Mwdhmsy])/;
		var interval_regex_ms = /(\d+(?:\.\d+)?)(ms)/;
		var matches = intervalString.match(interval_regex_ms);
		if (!matches) {
			matches = intervalString.match(interval_regex);
		}
		if (!matches) {
			throw new Error('Expecting a number followed by one of "y M w d h m s ms"');
		}

		var value = matches[1];
		var unit = matches[2];
		if (value % 1 !== 0) {
			if (unit === 'ms') {
				throw new Error('Cannot be smaller than the millisecond');
			}
			value = Math.round(kbn.intervals_in_seconds[unit] * value * 1000);
			unit = 'ms';

		}
		switch (unit) {
			case 'ms':
				unit = 'milliseconds';
				break;
			case 's':
				unit = 'seconds';
				break;
			case 'm':
				unit = 'minutes';
				break;
			case 'h':
				unit = 'hours';
				break;
			case 'd':
				unit = 'days';
				break;
			case 'w':
				unit = 'weeks';
				break;
			case 'M':
				unit = 'months';
				break;
			case 'y':
				unit = 'years';
				break;
			default:
				//console.log("Unknown interval ", intervalString);
				break;
		}

		return {
			"value": value,
			"unit": unit
		};

	};

	KairosDBDatasource.convertToShortTimeUnit = function (timeUnit) {
		var unit = timeUnit.unit;

		switch (unit) {
			case 'milliseconds':
				unit = 'ms';
				break;
			case 'seconds':
				unit = 's';
				break;
			case 'minutes':
				unit = 'm';
				break;
			case 'hours':
				unit = 'h';
				break;
			case 'days':
				unit = 'd';
				break;
			case 'weeks':
				unit = 'w';
				break;
			case 'months':
				unit = 'M';
				break;
			case 'years':
				unit = 'y';
				break;
			default:
				//console.log("Unknown interval ", intervalString);
				break;
		}
		return timeUnit.value + unit;

	};

	function convertToKairosTime(date, response_obj, start_stop_name) {
		var name;
		if (_.isString(date)) {
			if (date === 'now') {
				return;
			}
			else if (date.indexOf('now-') >= 0) {

				name = start_stop_name + "_relative";

				date = date.substring(4);
				var re_date = /(\d+)\s*(\D+)/;
				var result = re_date.exec(date);
				if (result) {
					var value = result[1];
					var unit = result[2];
					switch (unit) {
						case 'ms':
							unit = 'milliseconds';
							break;
						case 's':
							unit = 'seconds';
							break;
						case 'm':
							unit = 'minutes';
							break;
						case 'h':
							unit = 'hours';
							break;
						case 'd':
							unit = 'days';
							break;
						case 'w':
							unit = 'weeks';
							break;
						case 'M':
							unit = 'months';
							break;
						case 'y':
							unit = 'years';
							break;
						default:
							//console.log("Unknown date ", date);
							break;
					}
					response_obj[name] = {
						"value": value,
						"unit": unit
					};
					return;
				}
				//console.log("Unparseable date", date);
				return;
			}
			date = kbn.parseDate(date);
		}

		if (_.isDate(date)) {
			name = start_stop_name + "_absolute";
			response_obj[name] = date.getTime();
			return;
		}

		//console.log("Date is neither string nor date");
	}

	function endsWith(str, suffix) {
		return str.match(suffix + "$") == suffix;
	}

	////////////////////////////////////////////////////////////////////////
	return KairosDBDatasource;
});
