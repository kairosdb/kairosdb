var ROLLUP_URL = "/api/v1/rollups/";
var semaphore = false;
var metricList = null;


// todo convert between execution type and execution interval and back
// todo filter sampling
// todo convert groupby values to be more human readable
// todo validation
// todo how to display a complicated task
// todo adjust size of text boxes when in edit mode
// todo why does orderByFilter not work here but does in rollup.js???
// todo add Help to the filter sampling so user knows options

module.controller('simpleController', ['$scope', '$http', 'orderByFilter', 'KairosDBDatasource', simpleController]);
function simpleController($scope, $http, orderByFilter, KairosDBDatasource) {

	$scope.DEFAULT_TASK_NAME = "<task name>";
	$scope.DEFAULT_METRIC_NAME = "<metric name>";
	$scope.DEFAULT_SAVE_AS = "<new metric name>";
	$scope.METRIC_NAME_LIST_MAX_LENGTH = 20;
	$scope.DEFAULT_FILTER_SAMPLING = "1h";
	$scope.DEFAULT_GROUP_BY_TYPE = "tag";
	$scope.DEFAULT_GROUP_BY_VALUES = "<none>";

	$scope.tasks = [];
	$scope.executionTypes = ["Hourly", "Daily", "Weekly", "Monthly", "Yearly"];
	$scope.groupByTypes = ["tag", "time"];
	$scope.aggregators = ['avg', 'dev', 'max', 'min', 'sum', 'least_squares', 'count', 'percentile'];

	$http.get(ROLLUP_URL)
		.success(function (response) {

			if (response) {
				var rollupTasks = response;

				for (var i = 0; i < rollupTasks.length; i++) {
					// convert to a simpler model
					$scope.tasks.push($scope.toSimpleTask(rollupTasks[i])); // todo this loses data for a complex task check for complex and do something different to display
				}

				$scope.tasks = orderByFilter($scope.tasks, "name");
			}
		})
		.error(function (data, status, headers, config) {
			$scope.alert("Could not read list of roll-ups from server.", status, data);
		});

	$scope.onBlur = function (task) {
		// todo Implement validation
		//$scope.errors = validate(task);
		//
		//if (!$scope.hasErrors()) {
		//	$scope.tasks = orderByFilter($scope.tasks, "name");
		//
		//	if ($scope.isUnchanged(task))
		//		return;
		//
		//	$scope.saveRollupTask(task);
		//}
		$scope.saveTask(task)
	};

	$scope.setExecution = function (task, type) {
		task.executionType = type;
		$scope.onBlur(task);
	};

	$scope.setGroupBy = function (task, type) {
		task.groupByType = type;
		$scope.onBlur(task);
	};

	$scope.setFilter = function (task, filter) {
		task.filter = filter;
		$scope.onBlur(task);
	};

	// todo duplicated in rollup.js
	$scope.toHumanReadableTimeUnit = function (timeUnit) {
		if (timeUnit) {
			if (timeUnit.value == 1)
				return timeUnit.value + " " + timeUnit.unit.substring(0, timeUnit.unit.length - 1);
			else
				return timeUnit.value + " " + timeUnit.unit;
		}
	};

	$scope.addTask = function () {
		var task = {
			name: $scope.DEFAULT_TASK_NAME,
			metric_name: $scope.DEFAULT_METRIC_NAME,
			save_as: $scope.DEFAULT_SAVE_AS,
			executionType: "Daily",
			filter: "sum",
			filter_sampling: $scope.DEFAULT_FILTER_SAMPLING,
			group_by_type: $scope.DEFAULT_GROUP_BY_TYPE,
			group_by_values: $scope.DEFAULT_GROUP_BY_VALUES
		};
		$scope.tasks.push(task);
	};

	/**
	 Convert a task to the simple model used by the UI
	 */
	$scope.toSimpleTask = function (task) {
		var newTask = {};
		newTask.id = task.id;
		newTask.name = task.name;

		if (task.rollups.length > 0) {
			newTask.save_as = task.rollups[0].save_as;

			if (task.rollups[0].query) {
				if (task.rollups[0].query.metrics) {
					if (task.rollups[0].query.metrics.length > 0) {
						newTask.metric_name = task.rollups[0].query.metrics[0].name;
						//newTask.execution_interval = task.executionInterval;

						if (task.rollups[0].query.metrics[0].group_by && task.rollups[0].query.metrics[0].group_by.length > 0) {
							newTask.group_by_type = task.rollups[0].query.metrics[0].group_by[0].name;
							newTask.group_by_values = task.rollups[0].query.metrics[0].group_by[0].tags;
						}
						else {
							newTask.group_by_type = $scope.DEFAULT_GROUP_BY_TYPE;
							newTask.group_by_values = $scope.DEFAULT_GROUP_BY_VALUES;
						}

						if (task.rollups[0].query.metrics[0].aggregators.length > 0) {
							newTask.filter = task.rollups[0].query.metrics[0].aggregators[0].name;
							newTask.filter_sampling = KairosDBDatasource.convertToShortTimeUnit(task.rollups[0].query.metrics[0].aggregators[0].sampling);
						}
					}
				}
			}
		}
		return newTask;
	};

	/**
	 Converts a task from the simple module used by the UI to a the
	 real representation of a task.
	 */
	$scope.toRealTask = function (task) {
		var newTask = {};
		var rollups = [];
		var rollup = {};
		var query = {};
		var metrics = [];
		var metric = {};

		newTask.id = task.id;
		newTask.name = task.name;
		rollup.save_as = task.save_as;
		metric.name = task.metric_name;


		if (task.group_by_type && task.group_by_values) {
			var group_by = [];
			group_by.push({
				name: task.group_by_type,
				tags: task.group_by_value
			});
			metric.push(group_by);
		}

		var aggregators = [];
		var aggregator = {
			name: task.filter,
			sampling: KairosDBDatasource.convertToKairosInterval(task.filter_sampling)
		};
		aggregators.push(aggregator);
		metric.aggregators = aggregators;

		metrics.push(metric);
		query.metrics = metrics;
		rollup.query = query;
		rollups.push(rollup);
		newTask.rollups = rollups;

		// todo remove this
		newTask.execution_interval = {value: 1, unit: "hours"};

		// todo remove this
		query.start_relative = {value: 1, unit: "hours"};

		return newTask;
	};

	$scope.saveTask = function (task) {
		var realTask = $scope.toRealTask(task);

		var res = $http.post(ROLLUP_URL, realTask);
		res.success(function (data, status, headers, config) {
			task.id = data.id;

			currentDate = new Date();
			$scope.lastSaved = (currentDate.getHours() < 10 ? "0" + currentDate.getHours() : currentDate.getHours()) + ":" +
				(currentDate.getMinutes() < 10 ? "0" + currentDate.getMinutes() : currentDate.getMinutes()) + ":" +
				(currentDate.getSeconds() < 10 ? "0" + currentDate.getSeconds() : currentDate.getSeconds());

			// Flash Last Saved message
			$('#lastSaved').fadeOut('slow').fadeIn('slow').animate({opacity: 1.0}, 1000);

		});
		res.error(function (data, status, headers, config) {
			$scope.alert("Could not save query.", status, data);
		});
	};

	// todo duplicated in createController.js
	$scope.deleteRollupTask = function (task) {
		bootbox.confirm({
			size: 'medium',
			message: "Are you sure you want to delete the rollup?",
			callback: function (result) {
				if (result) {
					var res = $http.delete(ROLLUP_URL + task.id);
					res.success(function (data, status, headers, config) {
						var i = $scope.tasks.indexOf(task);
						if (i != -1) {
							$scope.tasks.splice(i, 1);
						}
					});
					res.error(function (data, status, headers, config) {
						$scope.alert("Failed to delete roll-up.", status, data);
					});
				}
			}
		});
	};

	$scope.suggestSaveAs = function (task) {
		if (!$scope.isMetricOrDefault(task) && $scope.isSaveAsEmptyOrDefault(task)) {
			task.save_as = task.metric_name + "_rollup";
		}
		$scope.onBlur(task);
	};

	$scope.isMetricOrDefault = function (task) {
		return !task.metric_name || task.metric_name == $scope.DEFAULT_METRIC_NAME;
	};

	$scope.isSaveAsEmptyOrDefault = function (task) {
		return !task.save_as || task.save_as == $scope.DEFAULT_SAVE_AS;
	};

	// todo duplicated in createController.js
	$scope.alert = function (message, status, data) {
		if (status) {


			var error = "";
			if (data && data.errors)
				error = data.errors;

			bootbox.alert({
				title: message,
				message: status + ":" + (error ? error : "" )
			});
		}
		else {
			bootbox.alert({
				message: message
			});
		}
	};

	// todo duplicated in createController.js
	$scope.suggestMetrics = function (metricName) {
		if (semaphore) {
			return;
		}
		var matcher = new RegExp(escapeRegex(metricName), 'i');
		if (!metricList) {
			$scope.updateMetricList();
		}
		if (_.isEmpty(metricName)) {
			return metricList;
		}

		var sublist = new Array($scope.METRIC_NAME_LIST_MAX_LENGTH);
		var j = 0;
		for (var i = 0; i < metricList.length; i++) {
			if (matcher.test(metricList[i])) {
				sublist[j] = metricList[i];
				j++;
				if (j === $scope.METRIC_NAME_LIST_MAX_LENGTH - 1) {
					break;
				}
			}
		}
		return sublist.slice(0, j);
	};

	// todo duplicated in createController.js
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

	// todo duplicated in createController.js
	var escapeRegex = function (e) {
		if (e) {
			return e.replace(/[\-\[\]{}()*+?.,\\\^$|#\s]/g, "\\$&")
		}
		return '';
	};

}
