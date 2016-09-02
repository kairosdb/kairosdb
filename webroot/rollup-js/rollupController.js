var ROLLUP_URL = "/api/v1/rollups/";
var semaphore = false;
var metricList = null;

module.controller('rollupController', ['$scope', '$http', '$uibModal', 'orderByFilter', 'KairosDBDatasource', simpleController]);
function simpleController($scope, $http, $uibModal, orderByFilter, KairosDBDatasource) {

	$scope.TOOLTIP_ADD_ROLL_UP = "Add new roll-up";
	$scope.TOOLTIP_PASTE_QUERY = "Paste query to create a roll-up";
	$scope.TOOLTIP_DELETE_ROLLUP = "Delete roll-up";
	$scope.TOOLTIP_SAMPLING_HELP = "Sampling help";
	$scope.TOOLTIP_TASK_NAME = "The name of the roll-up name";
	$scope.TOOLTIP_METRIC_NAME = "The metric the roll-up will query";
	$scope.TOOLTIP_SAVE_AS = "The new metric that will be created by the roll-up";
	$scope.TOOLTIP_EXECUTE = "How often the roll-up will be executed";
	$scope.TOOLTIP_GROUP_BY = "Groups the roll-up query by the tags";
	$scope.TOOLTIP_TAGS = "Narrows query down by tags";
	$scope.TOOLTIP_AGGREGATOR = "Aggregators perform an operation on data points and down samples";
	$scope.TOOLTIP_AGGREGATOR_SAMPLING = "Down sampling for the aggregator";
	$scope.TOOLTIP_INCOMPLETE = "Roll-up is not complete. Complete all grey-out fields.";
	$scope.TOOLTIP_COMPLEX = "This roll-up is a complex roll-up and cannot be managed from this UI.";

	$scope.EXECUTION_TYPES = ["Hourly", "Daily", "Weekly", "Monthly", "Yearly"];
	$scope.GROUP_BY_TYPES = ["tag", "time"];
	$scope.AGGREGATORS = ['avg', 'dev', 'max', 'min', 'sum', 'least_squares', 'count', 'percentile'];

	$scope.DEFAULT_TASK_NAME = "<roll-up name>";
	$scope.DEFAULT_METRIC_NAME = "<metric name>";
	$scope.DEFAULT_SAVE_AS = "<new metric name>";
	$scope.DEFAULT_EXECUTE = $scope.EXECUTION_TYPES[1];
	$scope.DEFAULT_AGGREGATOR = $scope.AGGREGATORS[4];
	$scope.METRIC_NAME_LIST_MAX_LENGTH = 20;
	$scope.DEFAULT_AGGREGATOR_SAMPLING = "1h";
	$scope.DEFAULT_GROUP_BY_TYPE = "tag";

	$scope.tasks = [];

	$http.get(ROLLUP_URL)
		.success(function (response) {

			if (response) {
				_.each(response, function(rollupTask){
					// convert to a simpler model
					var task = $scope.toSimpleTask(rollupTask);
					$scope.tasks.push(task);
					$scope.checkForIncompleteTask(task)
				});

				$scope.tasks = orderByFilter($scope.tasks, "name");
			}
		})
		.error(function (data, status, headers, config) {
			$scope.alert("Could not read list of roll-ups from server.", status, data);
		});

	$scope.onBlur = function (task) {
		$scope.errors = $scope.validate(task);
		$scope.checkForIncompleteTask(task);

		if (!$scope.hasErrors()) {
			$scope.saveTask(task)
		}
	};

	$scope.deleteSelected = function() {
		bootbox.confirm({
			size: 'medium',
			message: "Are you sure you want to delete the selected rollups?",
			callback: function (result) {
				if (result) {
					_.each($scope.tasks, function (task) {
						if (task.selected) {
							$scope.deleteTask(task)
						}
					});
					$scope.displayLastSaved();
				}
			}
		});
	};

	$scope.anyTasksSelected = function(){
		for(var i = 0; i < $scope.tasks.length; i++)
		{
			if ($scope.tasks[i].selected)
			{
				return true;
			}
		}
	};

	$scope.selectAllTasks = function(){
		_.each($scope.tasks, function(task){task.selected = true;});
	};

	$scope.selectNoTasks = function() {
		_.each($scope.tasks, function(task){task.selected = false;});
	};

	$scope.setExecution = function (task, type) {
		task.executionType = type;
		$scope.onBlur(task);
	};

	$scope.setGroupBy = function (task, type) {
		task.groupByType = type;
		$scope.onBlur(task);
	};

	$scope.setAggregator = function (task, aggregator) {
		task.aggregator = aggregator;
		$scope.onBlur(task);
	};

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
			aggregator: $scope.DEFAULT_AGGREGATOR,
			aggregator_sampling: $scope.DEFAULT_AGGREGATOR_SAMPLING,
			group_by_type: $scope.DEFAULT_GROUP_BY_TYPE
		};
		task.incomplete = true;
		$scope.tasks.push(task);
		return task;
	};

	/**
	 Convert a task to the simple model used by the UI
	 */
	$scope.toSimpleTask = function (task) {
		var newTask = {};
		newTask.id = task.id;
		newTask.name = task.name;
		newTask.executionType = $scope.convertFromExecutionInterval(task.execution_interval);

		if (task.rollups.length > 1){
			newTask.complex = true;
		}

		if (task.rollups.length > 0) {
			newTask.save_as = task.rollups[0].save_as;

			var query = task.rollups[0].query;
			if (query) {
				$scope.toSimpleQuery(query, newTask, task);
			}
		}
		return newTask;
	};

	$scope.toSimpleQuery = function(query, newTask) {
		if (query.metrics) {
			if (query.metrics.length > 0) {
				newTask.metric_name = query.metrics[0].name;

				if (query.metrics[0].group_by && query.metrics[0].group_by.length > 0) {
					newTask.group_by_type = query.metrics[0].group_by[0].name;
					newTask.group_by_values = query.metrics[0].group_by[0].tags.join(", ");
				}
				else {
					newTask.group_by_type = $scope.DEFAULT_GROUP_BY_TYPE;
				}

				if (query.metrics[0].aggregators.length > 0) {
					newTask.aggregator = query.metrics[0].aggregators[0].name;
					newTask.aggregator_sampling = KairosDBDatasource.convertToShortTimeUnit(query.metrics[0].aggregators[0].sampling);
				}

				if (query.metrics[0].tags){
					newTask.tags = query.metrics[0].tags;
				}
			}
		}
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

		if (task.tags){
			metric.tags = task.tags;
		}

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
			name: task.aggregator,
			sampling: KairosDBDatasource.convertToKairosInterval(task.aggregator_sampling)
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

	$scope.displayLastSaved = function()
	{
		currentDate = new Date();
		$scope.lastSaved = (currentDate.getHours() < 10 ? "0" + currentDate.getHours() : currentDate.getHours()) + ":" +
			(currentDate.getMinutes() < 10 ? "0" + currentDate.getMinutes() : currentDate.getMinutes()) + ":" +
			(currentDate.getSeconds() < 10 ? "0" + currentDate.getSeconds() : currentDate.getSeconds());

		// Flash Last Saved message
		$('#lastSaved').fadeOut('slow').fadeIn('slow').animate({opacity: 1.0}, 1000);
	};

	$scope.saveTask = function (task) {
		var realTask = $scope.toRealTask(task);

		var res = $http.post(ROLLUP_URL, realTask);
		res.success(function (data, status, headers, config) {
			task.id = data.id;

			$scope.displayLastSaved();
		});
		res.error(function (data, status, headers, config) {
			$scope.alert("Could not save query.", status, data);
		});
	};

	$scope.deleteTask = function (task) {
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
	};

	$scope.deleteTaskWithConfirm = function (task) {
		bootbox.confirm({
			size: 'medium',
			message: "Are you sure you want to delete the rollup?",
			callback: function (result) {
				if (result) {
					$scope.deleteTask(task);
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

	$scope.suggestTagValues = function (task) {
		return KairosDBDatasource.performTagSuggestQuery(task.metric_name, 'value', task.currentTagKey);
	};

	$scope.escapeRegex = function (e) {
		if (e) {
			return e.replace(/[\-\[\]{}()*+?.,\\\^$|#\s]/g, "\\$&")
		}
		return '';
	};

	$scope.pasteQuery = function (task, rollup, edit) {
		var modalInstance = $uibModal.open({
			templateUrl: 'rollup-paste-query.html?cacheBust=' + Math.random().toString(36).slice(2), //keep dialog from caching
			controller: 'PasteQueryCtrl',
			size: 'md',
			backdrop: 'static', // disable closing of dialog with click away
			keyboard: false // disable closing dialog with ESC
		});

		modalInstance.result.then(
			function (response) {
				var newTask = $scope.addTask();
				newTask.name = response.name;
				newTask.executionType = response.executionType;
				$scope.toSimpleQuery(response.query, newTask);
				$scope.suggestSaveAs(newTask);
			});
	};

	$scope.checkForIncompleteTask = function(task){
		if (!task.name || _.isEmpty(task.name) || task.name == $scope.DEFAULT_TASK_NAME) {
			task.incomplete = true;
		}
		else if (!task.metric_name|| _.isEmpty(task.metric_name) || task.metric_name == $scope.DEFAULT_METRIC_NAME) {
			task.incomplete = true;
		}
		else if (!task.save_as|| _.isEmpty(task.save_as) || task.save_as == $scope.DEFAULT_SAVE_AS) {
			task.incomplete = true;
		}
		else if (!task.aggregator_sampling || _.isEmpty(task.aggregator_sampling)) {
			task.incomplete = true;
		}
		else {
			task.incomplete = false;
		}
	};

	$scope.hasErrors = function () {
		return !_.isEmpty($scope.errors);
	};

	$scope.validate = function(task) {
		var errs = {};

		if (!task.name || _.isEmpty(task.name)) {
			errs.name = "Name cannot be empty.";
			task.name = $scope.DEFAULT_TASK_NAME;
			$scope.alert(errs.name);
		}
		if (!task.metric_name|| _.isEmpty(task.metric_name)) {
			errs.name = "Metric cannot be empty.";
			task.metric_name = $scope.DEFAULT_METRIC_NAME;
			$scope.alert(errs.name);
		}
		if (!task.save_as|| _.isEmpty(task.save_as)) {
			errs.name = "Save As cannot be empty.";
			task.save_as = $scope.DEFAULT_SAVE_AS;
			$scope.alert(errs.name);
		}
		if (!task.aggregator_sampling|| _.isEmpty(task.aggregator_sampling)) {
			errs.name = "aggregator Sampling cannot be empty.";
			task.aggregator_sampling =  $scope.DEFAULT_AGGREGATOR_SAMPLING;
			$scope.alert(errs.name);
		}
		if (task.aggregator_sampling) {
			try {
				KairosDBDatasource.convertToKairosInterval(task.aggregator_sampling);
			}
			catch(err){
				errs.name = "Invalid aggregator Sampling: " + err.message;
				$scope.alert(errs.name);
			}
		}

		return errs;
	}
}
