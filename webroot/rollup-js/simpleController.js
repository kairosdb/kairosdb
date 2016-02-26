var ROLLUP_URL = "/api/v1/rollups/";
var semaphore = false;
var metricList = null;

// todo how to display a complicated task

module.controller('simpleController', ['$scope', '$http', 'orderByFilter', 'KairosDBDatasource', simpleController]);
function simpleController($scope, $http, orderByFilter, KairosDBDatasource) {

	$scope.EXECUTION_TYPES = ["Hourly", "Daily", "Weekly", "Monthly", "Yearly"];
	$scope.GROUP_BY_TYPES = ["tag", "time"];
	$scope.FILTERS = ['avg', 'dev', 'max', 'min', 'sum', 'least_squares', 'count', 'percentile'];

	$scope.DEFAULT_TASK_NAME = "<task name>";
	$scope.DEFAULT_METRIC_NAME = "<metric name>";
	$scope.DEFAULT_SAVE_AS = "<new metric name>";
	$scope.DEFAULT_EXECUTE = $scope.EXECUTION_TYPES[1];
	$scope.DEFAULT_FILTER = $scope.FILTERS[4];
	$scope.METRIC_NAME_LIST_MAX_LENGTH = 20;
	$scope.DEFAULT_FILTER_SAMPLING = "1h";
	$scope.DEFAULT_GROUP_BY_TYPE = "tag";

	$scope.tasks = [];

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
		$scope.errors = $scope.validate(task);

		if (!$scope.hasErrors()) {
			//$scope.tasks = orderByFilter($scope.tasks, "name");
			$scope.saveTask(task)
		}
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

	$scope.setGroupValues = function(task){
		task.groupByEdit=false;
		if (!task.group_by_values){
			task.group_by_values = task.group_by_value_suggest;
		}
		else {
			task.group_by_values += ", " + task.group_by_value_suggest;
		}
		task.group_by_value_suggest = "";
		$scope.onBlur(task);
	};

	$scope.removeGroupValues = function(task){
		if (task.group_by_values){
			task.group_by_values = "";
		}
		$scope.onBlur(task);
	};

	$scope.cancelGroupValues = function(task){
		task.groupByEdit=false;
		task.group_by_value_suggest = "";
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
			executionType: $scope.DEFAULT_EXECUTE,
			filter: $scope.DEFAULT_FILTER,
			filter_sampling: $scope.DEFAULT_FILTER_SAMPLING,
			group_by_type: $scope.DEFAULT_GROUP_BY_TYPE
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
						newTask.executionType = $scope.convertFromExecutionInterval(task.execution_interval);

						if (task.rollups[0].query.metrics[0].group_by && task.rollups[0].query.metrics[0].group_by.length > 0) {
							newTask.group_by_type = task.rollups[0].query.metrics[0].group_by[0].name;
							newTask.group_by_values = task.rollups[0].query.metrics[0].group_by[0].tags.join(", ");
						}
						else {
							newTask.group_by_type = $scope.DEFAULT_GROUP_BY_TYPE;
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


		if (task.group_by_type && task.group_by_values && task.group_by_values.length > 0) {
			var group_by = [];
			group_by.push({
				name: task.group_by_type,
				tags: task.group_by_values.split(", ")
			});
			metric.group_by = group_by;
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

		newTask.execution_interval = {value: 1, unit: $scope.convertToExecutionInterval(task.executionType)};

		query.start_relative = {value: 1, unit: "hours"};

		return newTask;
	};

	$scope.convertFromExecutionInterval = function(executionInterval){
		switch(executionInterval.unit.toLowerCase()){
			case 'milliseconds':
			case 'seconds':
			case 'minutes':
			case 'hours':
				return $scope.EXECUTION_TYPES[0];
			case 'days':
				return $scope.EXECUTION_TYPES[1];
			case 'weeks':
				return $scope.EXECUTION_TYPES[2];
			case 'months':
				return $scope.EXECUTION_TYPES[3];
			case 'years':
				return $scope.EXECUTION_TYPES[4];
			default:
				$scope.alert("Invalid execution interval specified: " + executionInterval.unit);
		}
	};

	$scope.convertToExecutionInterval = function(executionType){
		switch(executionType){
			case $scope.EXECUTION_TYPES[0]:
				return 'hours';
			case $scope.EXECUTION_TYPES[1]:
				return "days";
			case $scope.EXECUTION_TYPES[2]:
				return "weeks";
			case $scope.EXECUTION_TYPES[3]:
				return "months";
			case $scope.EXECUTION_TYPES[4]:
				return "years";
			default:
				$scope.alert("Invalid execution interval specified: " + executionInterval.unit);
		}
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
					if (task.id) {
						var res = $http.delete(ROLLUP_URL + task.id);
						res.success(function (data, status, headers, config) {
							$scope.removeTaskFromTasks(task);
						});
						res.error(function (data, status, headers, config) {
							$scope.alert("Failed to delete roll-up.", status, data);
						});
					}
					else {
						// Task has never been saved
						$scope.removeTaskFromTasks(task);
						$scope.$apply();
					}
				}
			}
		});
	};

	$scope.removeTaskFromTasks = function(task) {
		for(var i = 0; i < $scope.tasks.length; i++){
			if (_.isEqual($scope.tasks[i], task)){
				$scope.tasks.splice(i, 1);
				break;
			}
		}
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
		var matcher = new RegExp($scope.escapeRegex(metricName), 'i');
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

	$scope.suggestTagKeys = function (task) {
		return KairosDBDatasource.performTagSuggestQuery(task.metric_name, 'key', '');

	};

	// todo duplicated in createController.js
	$scope.escapeRegex = function (e) {
		if (e) {
			return e.replace(/[\-\[\]{}()*+?.,\\\^$|#\s]/g, "\\$&")
		}
		return '';
	};

	// todo duplicated in createController.js
	$scope.hasErrors = function () {
		return !_.isEmpty($scope.errors);
	};

	$scope.validate = function(task) {
		var errs = {};

		if (!task.name || _.isEmpty(task.name)) {
			errs.name = "Name cannot be empty.";
			$scope.alert(errs.name);
		}
		if (!task.metric_name|| _.isEmpty(task.metric_name)) {
			errs.name = "Metric cannot be empty.";
			$scope.alert(errs.name);
		}
		if (!task.save_as|| _.isEmpty(task.save_as)) {
			errs.name = "Save As cannot be empty.";
			$scope.alert(errs.name);
		}
		if (!task.filter_sampling|| _.isEmpty(task.filter_sampling)) {
			errs.name = "Filter Sampling cannot be empty.";
			$scope.alert(errs.name);
		}
		if (task.filter_sampling) {
			try {
				KairosDBDatasource.convertToKairosInterval(task.filter_sampling);
			}
			catch(err){
				errs.name = "Invalid Filter Sampling: " + err.message;
				$scope.alert(errs.name);
			}
		}

		return errs;
	}
}
