var metricList = null;

module.controller('KairosDBTargetCtrl', ['$scope', '$modalInstance', 'KairosDBDatasource', 'rollup', KairosDBTargetCtrl]);

function KairosDBTargetCtrl($scope, $modalInstance, KairosDBDatasource, rollup) {

	$scope.target = {};
	$scope.target.metric = "";
	$scope.target.downsampling = '(NONE)';
	$scope.errors = {};
	$scope.target.start_relative = {};

	$scope.init = function () {
		if (rollup) {
			convertFromQueryToTarget(rollup);
		}

		if (!$scope.target.downsampling) {
			$scope.target.downsampling = 'avg';
		}
		$scope.errors = validateTarget($scope.target);
		$scope.updateMetricList(function () {
			$scope.suggestMetrics()
		});

		$scope.target.start_relative = $scope.relativeStartTimes[6];
	};

	$scope.targetBlur = function () {
		$scope.errors = validateTarget($scope.target);
		if (!_.isEqual($scope.oldTarget, $scope.target) && _.isEmpty($scope.errors)) {
			$scope.oldTarget = angular.copy($scope.target);
			$scope.suggestTagKeys();
		}
	};

	$scope.relativeStartTimes = [
		{value: 1, unit: "minutes", label: "1 minute"},
		{value: 5, unit: "minutes", label: "5 minutes"},
		{value: 10, unit: "minutes", label: "10 minutes"},
		{value: 15, unit: "minutes", label: "15 minutes"},
		{value: 20, unit: "minutes", label: "20 minutes"},
		{value: 30, unit: "minutes", label: "30 minutes"},
		{value: 1, unit: "hours", label: "1 hour"},
		{value: 6, unit: "hours", label: "6 hours"},
		{value: 1, unit: "days", label: "1 day"},
		{value: 1, unit: "weeks", label: "1 week"}
	];

	function convertFromQueryToTarget(rollup) {
		// todo handle relative start time


		// Metric Name
		$scope.target.metric = rollup.query.metrics[0].name;

		// Aggregators
		var aggregators = rollup.query.metrics[0].aggregators;
		if (aggregators) {
			$scope.target.horizontalAggregators = [];
			_.each(aggregators, function (aggregator) {
				$scope.addHorizontalAggregatorMode = true;
				$scope.target.currentHorizontalAggregatorName = aggregator.name;
				$scope.setHorizontalAggregationInput();
				$scope.target.horAggregator = {};

				if (aggregator.hasOwnProperty('sampling')) {
					$scope.target.horAggregator.samplingRate = KairosDBDatasource.convertToShortTimeUnit(aggregator.sampling);
					$scope.target.horAggregator.alignStartTime = aggregator.align_start_time ? aggregator.align_start_time : false;
				}
				if ($scope.hasUnit) {
					$scope.target.horAggregator.unit = aggregator.unit;
				}
				if ($scope.hasFactor) {
					if (aggregator.name == 'div')
						$scope.target.horAggregator.factor = aggregator.divisor;
					if (aggregator.name == 'scale')
						$scope.target.horAggregator.factor = aggregator.factor;
				}
				if ($scope.hasPercentile) {
					$scope.target.horAggregator.percentile = aggregator.percentile;
				}
				$scope.addHorizontalAggregator();
			});
		}

		// Tags
		var tags = rollup.query.metrics[0].tags;
		if (tags) {
			_.each(tags, function (value, key) {
				_.each(value, function (tagValue) {
					$scope.addFilterTagMode = true;
					$scope.target.currentTagKey = key;
					$scope.target.currentTagValue = tagValue;
					$scope.addFilterTag();
				});
			});
		}

		// Group By
		var groupBy = rollup.query.metrics[0].group_by;
		if (groupBy) {
			_.each(groupBy, function (group) {
				$scope.target.groupBy = {};
				$scope.target.currentGroupByType = group.name;
				if (group.name == "tag") {
					_.each(group.tags, function (tag) {
						$scope.addGroupByMode = true;
						$scope.isTagGroupBy = true;
						$scope.isTimeGroupBy = false;
						$scope.isValueGroupBy = false;
						$scope.target.groupBy.tagKey = tag;
						$scope.addGroupBy();
					});
				}
				else {
					$scope.target.groupBy.name = group.name;

					if (group.name == "time") {
						$scope.addGroupByMode = true;
						$scope.isTagGroupBy = false;
						$scope.isTimeGroupBy = true;
						$scope.isValueGroupBy = false;
						$scope.target.groupBy.timeInterval = KairosDBDatasource.convertToShortTimeUnit(group.range_size);
						$scope.target.groupBy.groupCount = group.group_count;

					}
					if (group.name == "value") {
						$scope.addGroupByMode = true;
						$scope.isTagGroupBy = false;
						$scope.isTimeGroupBy = false;
						$scope.isValueGroupBy = true;
						$scope.target.groupBy.valueRange = group.range_size;
					}
					$scope.addGroupBy();
				}
			});
		}
	}

	$scope.ok = function () {
		var result = {};
		result.query = {};
		result.query.start_relative = $scope.target.start_relative;
		result.query.metrics = [];
		var metric = KairosDBDatasource.convertTargetToQuery({}, $scope.target);
		result.query.metrics.push(metric);
		$modalInstance.close(result);
	};

	$scope.cancel = function () {
		$modalInstance.dismiss('cancel');
	};

	$scope.getValues = function (object) {
		return _.values(object);
	};

	//////////////////////////////
	// SUGGESTION QUERIES
	//////////////////////////////

	var escapeRegex = function (e) {
		return e.replace(/[\-\[\]{}()*+?.,\\\^$|#\s]/g, "\\$&")
	};

	var semaphore = false;

	$scope.suggestMetrics = function () {
		if (semaphore) {
			return;
		}
		var MAXSIZE = 10;
		var matcher = new RegExp(escapeRegex($scope.target.metric), 'i');
		if (!metricList) {
			$scope.updateMetricList();
		}
		if (_.isEmpty($scope.target.metric)) {
			return metricList;
		}

		var sublist = new Array(MAXSIZE);
		var j = 0;
		for (var i = 0; i < metricList.length; i++) {
			if (matcher.test(metricList[i])) {
				sublist[j] = metricList[i];
				j++;
				if (j === MAXSIZE - 1) {
					break;
				}
			}
		}
		return sublist.slice(0, j);
	};

	$scope.updateMetricList = function (callback) {
		$scope.metricListLoading = true;
		semaphore = true;
		metricList = [];
		KairosDBDatasource.performMetricSuggestQuery().then(function (series) {
			metricList = series;
			$scope.metricListLoading = false;
			semaphore = false;

			if (callback) {
				callback();
			}
		});
	};

	$scope.suggestTagKeys = function () {
		return KairosDBDatasource.performTagSuggestQuery($scope.target.metric, 'key', '');

	};

	$scope.suggestTagValues = function () {
		return KairosDBDatasource.performTagSuggestQuery($scope.target.metric, 'value', $scope.target.currentTagKey);
	};

	//////////////////////////////
	// FILTER by TAG
	//////////////////////////////

	$scope.addFilterTag = function () {
		if (!$scope.addFilterTagMode) {
			$scope.addFilterTagMode = true;
			$scope.validateFilterTag();
			return;
		}

		if (!$scope.target.tags) {
			$scope.target.tags = {};
		}

		$scope.validateFilterTag();
		if (!$scope.errors.tags) {
			if (!_.has($scope.target.tags, $scope.target.currentTagKey)) {
				$scope.target.tags[$scope.target.currentTagKey] = [];
			}
			if (!_.contains($scope.target.tags[$scope.target.currentTagKey], $scope.target.currentTagValue)) {
				$scope.target.tags[$scope.target.currentTagKey].push($scope.target.currentTagValue);
				$scope.targetBlur();
			}
			$scope.target.currentTagKey = '';
			$scope.target.currentTagValue = '';
		}

		$scope.addFilterTagMode = false;
	};

	$scope.removeFilterTag = function (key) {
		delete $scope.target.tags[key];
		if (_.size($scope.target.tags) === 0) {
			$scope.target.tags = null;
		}
		$scope.targetBlur();
	};

	$scope.validateFilterTag = function () {
		$scope.errors.tags = null;
		if (!$scope.target.currentTagKey || !$scope.target.currentTagValue) {
			$scope.errors.tags = "You must specify a tag name and value.";
		}
	};

	//////////////////////////////
	// GROUP BY
	//////////////////////////////

	$scope.addGroupBy = function () {
		if (!$scope.addGroupByMode) {
			$scope.addGroupByMode = true;
			$scope.target.currentGroupByType = 'tag';
			$scope.isTagGroupBy = true;
			$scope.validateGroupBy();
			return;
		}
		$scope.validateGroupBy();
		// nb: if error is found, means that user clicked on cross : cancels input
		if (_.isEmpty($scope.errors.groupBy)) {
			if ($scope.isTagGroupBy) {
				if (!$scope.target.groupByTags) {
					$scope.target.groupByTags = [];
				}
				//console.log($scope.target.groupBy.tagKey);
				if (!_.contains($scope.target.groupByTags, $scope.target.groupBy.tagKey)) {
					$scope.target.groupByTags.push($scope.target.groupBy.tagKey);
					$scope.targetBlur();
				}
				$scope.target.groupBy.tagKey = '';
			}
			else {
				if (!$scope.target.nonTagGroupBys) {
					$scope.target.nonTagGroupBys = [];
				}
				var groupBy = {
					name: $scope.target.currentGroupByType
				};
				if ($scope.isValueGroupBy) {
					groupBy.range_size = $scope.target.groupBy.valueRange;
				}
				else if ($scope.isTimeGroupBy) {
					groupBy.range_size = $scope.target.groupBy.timeInterval;
					groupBy.group_count = $scope.target.groupBy.groupCount;
				}
				$scope.target.nonTagGroupBys.push(groupBy);
			}
			$scope.targetBlur();
		}
		$scope.isTagGroupBy = false;
		$scope.isValueGroupBy = false;
		$scope.isTimeGroupBy = false;
		$scope.addGroupByMode = false;
	};

	$scope.removeGroupByTag = function (index) {
		$scope.target.groupByTags.splice(index, 1);
		if (_.size($scope.target.groupByTags) === 0) {
			$scope.target.groupByTags = null;
		}
		$scope.targetBlur();
	};

	$scope.removeNonTagGroupBy = function (index) {
		$scope.target.nonTagGroupBys.splice(index, 1);
		if (_.size($scope.target.nonTagGroupBys) === 0) {
			$scope.target.nonTagGroupBys = null;
		}
		$scope.targetBlur();
	};

	$scope.changeGroupByInput = function () {
		$scope.isTagGroupBy = $scope.target.currentGroupByType === 'tag';
		$scope.isValueGroupBy = $scope.target.currentGroupByType === 'value';
		$scope.isTimeGroupBy = $scope.target.currentGroupByType === 'time';
		$scope.validateGroupBy();
	};

	$scope.validateGroupBy = function () {
		delete $scope.errors.groupBy;
		var errors = {};
		$scope.isGroupByValid = true;
		if ($scope.isTagGroupBy) {
			if (!$scope.target.groupBy.tagKey) {
				$scope.isGroupByValid = false;
				errors.tagKey = 'You must supply a tag name';
			}
		}
		if ($scope.isValueGroupBy) {
			if (!$scope.target.groupBy.valueRange || !isInt($scope.target.groupBy.valueRange)) {
				errors.valueRange = "Range must be an integer";
				$scope.isGroupByValid = false;
			}
		}
		if ($scope.isTimeGroupBy) {
			try {
				KairosDBDatasource.convertToKairosInterval($scope.target.groupBy.timeInterval);
			} catch (err) {
				errors.timeInterval = err.message;
				$scope.isGroupByValid = false;
			}
			if (!$scope.target.groupBy.groupCount || !isInt($scope.target.groupBy.groupCount)) {
				errors.groupCount = "Group count must be an integer";
				$scope.isGroupByValid = false;
			}
		}

		if (!_.isEmpty(errors)) {
			$scope.errors.groupBy = errors;
		}
	};

	function isInt(n) {
		return parseInt(n) % 1 === 0;
	}

	//////////////////////////////
	// AGGREGATION
	//////////////////////////////

	$scope.getAggregatorKeys = function (object) {
		return _.keys(_.omit(object, 'name'));
	};

	$scope.addHorizontalAggregator = function () {
		if (!$scope.addHorizontalAggregatorMode) {
			$scope.addHorizontalAggregatorMode = true;
			$scope.target.currentHorizontalAggregatorName = 'avg';
			$scope.hasSamplingRate = true;
			$scope.validateHorizontalAggregator();
			return;
		}

		$scope.validateHorizontalAggregator();
		// nb: if error is found, means that user clicked on cross : cancels input
		if (_.isEmpty($scope.errors.horAggregator)) {
			if (!$scope.target.horizontalAggregators) {
				$scope.target.horizontalAggregators = [];
			}
			var aggregator = {
				name: $scope.target.currentHorizontalAggregatorName
			};
			if ($scope.hasSamplingRate) {
				aggregator.sampling_rate = $scope.target.horAggregator.samplingRate;
				aggregator.align_start_time = $scope.target.horAggregator.alignStartTime;
			}
			if ($scope.hasUnit) {
				aggregator.unit = $scope.target.horAggregator.unit;
			}
			if ($scope.hasFactor) {
				aggregator.factor = $scope.target.horAggregator.factor;
			}
			if ($scope.hasPercentile) {
				aggregator.percentile = $scope.target.horAggregator.percentile;
			}
			$scope.target.horizontalAggregators.push(aggregator);
			$scope.targetBlur();
		}

		$scope.addHorizontalAggregatorMode = false;
		$scope.hasSamplingRate = false;
		$scope.hasUnit = false;
		$scope.hasFactor = false;
		$scope.hasPercentile = false;

	};

	$scope.removeHorizontalAggregator = function (index) {
		$scope.target.horizontalAggregators.splice(index, 1);
		if (_.size($scope.target.horizontalAggregators) === 0) {
			$scope.target.horizontalAggregators = null;
		}

		$scope.targetBlur();
	};

	$scope.setHorizontalAggregationInput = function () {
		$scope.hasSamplingRate = _.contains(['avg', 'dev', 'max', 'min', 'sum', 'least_squares', 'count', 'percentile'],
			$scope.target.currentHorizontalAggregatorName);
		$scope.hasUnit = _.contains(['sampler', 'rate'], $scope.target.currentHorizontalAggregatorName);
		$scope.hasFactor = _.contains(['div', 'scale'], $scope.target.currentHorizontalAggregatorName);
		$scope.hasPercentile = 'percentile' === $scope.target.currentHorizontalAggregatorName;
	};

	$scope.changeHorAggregationInput = function () {
		$scope.setHorizontalAggregationInput();
		$scope.validateHorizontalAggregator();
	};

	$scope.validateHorizontalAggregator = function () {
		delete $scope.errors.horAggregator;
		var errors = {};
		$scope.isAggregatorValid = true;
		if ($scope.hasSamplingRate) {
			try {
				KairosDBDatasource.convertToKairosInterval($scope.target.horAggregator.samplingRate);
			} catch (err) {
				errors.samplingRate = err.message;
				$scope.isAggregatorValid = false;
			}
		}
		if ($scope.hasFactor) {
			if (!$scope.target.horAggregator.factor) {
				errors.factor = 'You must supply a numeric value for this aggregator';
				$scope.isAggregatorValid = false;
			}
			else if (parseInt($scope.target.horAggregator.factor) === 0 && $scope.target.currentHorizontalAggregatorName === 'div') {
				errors.factor = 'Cannot divide by 0';
				$scope.isAggregatorValid = false;
			}
		}
		if ($scope.hasPercentile) {
			if (!$scope.target.horAggregator.percentile ||
				$scope.target.horAggregator.percentile <= 0 ||
				$scope.target.horAggregator.percentile > 1) {
				errors.percentile = 'Percentile must be between 0 and 1';
				$scope.isAggregatorValid = false;
			}
		}

		if (!_.isEmpty(errors)) {
			$scope.errors.horAggregator = errors;
		}
	};

	$scope.alert = function (message) {
		alert(message);
	};

	$scope.hasErrors = function () {
		return !_.isEmpty($scope.errors);
	};

	//////////////////////////////
	// VALIDATION
	//////////////////////////////

	function validateTarget(target) {
		var errs = {};

		if (!target.metric) {
			errs.metric = "You must supply a metric name.";
		}

		try {
			if (target.sampling) {
				KairosDBDatasource.convertToKairosInterval(target.sampling);
			}
		} catch (err) {
			errs.sampling = err.message;
		}

		return errs;
	}
}
