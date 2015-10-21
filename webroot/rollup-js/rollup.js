var module = angular.module('rollupApp', ['mgcrea.ngStrap', 'mgcrea.ngStrap.alert',
	'mgcrea.ngStrap.tooltip', 'ui.bootstrap.modal', 'template/modal/backdrop.html',
	'template/modal/window.html']);

module.controller('rollupController', function ($scope, $http, $uibModal) {
	$http.get("/api/v1/rollups/rollup") //todo don't hard code
		.success(function (response) {

			if (response)
			// todo sort by task name
				$scope.tasks = response;
			else
				$scope.tasks = [];

			console.log(response);
		})
		.error(function (data, status, headers, config) {
			alert("failure message: " + JSON.stringify({data: data}));
		});

	$scope.toHumanReadableCron = function (schedule) {
		return prettyCron.toString(schedule);
	};

	$scope.toHumanReadableTimeUnit = function (timeUnit) {
		if (timeUnit.value == 1)
			return timeUnit.value + " " + timeUnit.unit.substring(0, timeUnit.unit.length - 1);
		else
			return timeUnit.value + " " + timeUnit.unit;
	};

	$scope.toTql = function (query) {
		tql = "SELECT " + query.query.metrics[0].name;
		tql += " FROM " + $scope.toHumanReadableTimeUnit(query.query.start_relative) + " to now";  // todo fix for non-relative times

		// Tags
		var first = true;
		if (query.query.metrics[0].tags && Object.keys(query.query.metrics[0].tags).length > 0) {
			tql += " WHERE ";

			var tags = query.query.metrics[0].tags;
			for (var tag in tags) {
				if (first)
					first = false;
				else {
					tql += ", ";
				}

				//noinspection JSUnfilteredForInLoop
				tql += tag + ' = [' + tags[tag] + ']';
			}
		}

		// Aggregators
		if (query.query.metrics[0].aggregators && query.query.metrics[0].aggregators.length > 0) {
			tql += " AGGREGATE ";

			first = true;
			query.query.metrics[0].aggregators.forEach(function (aggregator) {
				if (first)
					first = false;
				else
					tql += " | ";

				tql += $scope.toHumanReadableAggregator(aggregator);
			});
		}

		// todo GroupBy

		return tql;


		//{"query":{"start_relative":{"value":1,"unit":"hours"},"metrics":[{"aggregators":[{"name":"sum","sampling":{"value":1,"unit":"minutes"}}],"name":"tap.event-indexer.foobar"}]},"save_as":"e"}
	};

	$scope.toHumanReadableAggregator = function (aggregator) {
		var result = aggregator.name + '(';
		if (aggregator.sampling) {
			result += $scope.toHumanReadableTimeUnit(aggregator.sampling) + ", ";
			result += aggregator.sampling.align_start_time ? aggregator.sampling.align_start_time : false;
		}
		else {
			_.each(_.values(_.omit(aggregator, 'name')), function (value) {
				result += value + ",";
			});
			result = result.substring(0, result.length - 1); // Remove trailing comma

		}
		result += ')';
		return result;
	};

	$scope.relativeStartTimes = [
		{value: 1, unit: "minutes"},
		{value: 5, unit: "minutes"},
		{value: 10, unit: "minutes"},
		{value: 15, unit: "minutes"},
		{value: 20, unit: "minutes"},
		{value: 30, unit: "minutes"},
		{value: 1, unit: "hours"},
		{value: 6, unit: "hours"},
		{value: 1, unit: "days"},
		{value: 1, unit: "weeks"}
	];

	$scope.executeTimes = [
		'* * * * * ?',    // every minute
		'5 * * * * ?',    // every 5 minutes
		'10 * * * * ?',   // every 10 minutes
		'15 * * * * ?',   // every 15 minutes
		'30 * * * * ?',   // every 30 minutes
		'* 1 * * * ?',    // every 1 hour
		'* 6 * * * ?',    // every 6 hours
		'0 0 * * * ?',    // every day
		'0 0 * * sat ?'   // once a week
	];

	$scope.aggregators = ["sum", "avg", "min", "max"];

	$scope.samplingTimes = [
		{value: 1, unit: "minutes"},
		{value: 5, unit: "minutes"},
		{value: 10, unit: "minutes"},
		{value: 15, unit: "minutes"},
		{value: 20, unit: "minutes"},
		{value: 30, unit: "minutes"},
		{value: 1, unit: "hours"},
		{value: 6, unit: "hours"},
		{value: 1, unit: "days"},
		{value: 1, unit: "weeks"}
	];

	////var escapeRegex = function(e) {
	////	return e.replace(/[\-\[\]{}()*+?.,\\\^$|#\s]/g,"\\$&")
	////};
	////
	////var semaphore = false;
	////$scope.suggestMetrics = function(query, callback) {
	////	if(semaphore) {
	////		return;
	////	}
	////	var MAXSIZE = 10;
	////	var matcher = new RegExp(escapeRegex(query), 'i');
	////	if (!metricList) {
	////		$scope.updateMetricList(callback);
	////	}
	////	else {
	////		var sublist = new Array(MAXSIZE);
	////		var j = 0;
	////		for(var i=0;i<metricList.length;i++){
	////			if (matcher.test(metricList[i])) {
	////				sublist[j] = metricList[i];
	////				j++;
	////				if(j===MAXSIZE-1){
	////					break;
	////				}
	////			}
	////		}
	////		sublist = sublist.slice(0,j);
	////		callback(sublist);
	////	}
	////};
	//
	//$scope.selectedMetricName = "";
	//$scope.metricNames = ['kairosdb.metric1', 'kairosdb.metric2', 'foobar'];
	//
	//$scope.updateMetricList = function(callback) {
	//	$scope.metricListLoading = true;
	//	semaphore = true;
	//	metricList = [];
	//	$scope.datasource.performMetricSuggestQuery().then(function(series) {
	//		metricList = series;
	//		$scope.metricListLoading = false;
	//		semaphore = false;
	//		if(callback)
	//			callback(_.sortBy(metricList));
	//	});
	//};
	//
	//$scope.performMetricSuggestQuery = function() {
	//	var options = {
	//		url : this.url + '/api/v1/metricnames',
	//		method : 'GET'
	//	};
	//	return $http(options).then(function(results) {
	//		if (!results.data) {
	//			return [];
	//		}
	//		return results.data.results;
	//	});
	//};

	$scope.scheduleModified = function (task, item) {
		task.schedule = item;
	};

	$scope.startTimeModified = function (rollup, item) {
		rollup.tasks[0].query.start_relative = item;
	};

	$scope.aggregatorModified = function (rollup, item) {
		rollup.tasks[0].query.metrics[0].aggregators[0].name = item;
	};

	$scope.aggregatorSamplingModified = function (rollup, item) {
		rollup.tasks[0].query.metrics[0].aggregators[0].sampling = item;
	};

	$scope.addRollupTask = function () {
		var task = {schedule: '15 * * * * ?', rollups: []};
		task.edit = true;
		$scope.tasks.push(task);

	};

	$scope.editRollupTask = function (task) {
		task.edit = true;
	};

	$scope.saveRollupTask = function (task) {
		$scope.postNewRollupTask(task);
		task.edit = false;
	};

	$scope.deleteRollupTask = function (task) {
		var res = $http.delete('/api/v1/rollups/delete/' + task.id); // todo don't hardcode?
		res.success(function (data, status, headers, config) {
			var i = $scope.tasks.indexOf(task);
			if (i != -1) {
				$scope.tasks.splice(i, 1);
			}
			console.log(status);
		});
		res.error(function (data, status, headers, config) {
			alert("failure message: " + JSON.stringify({data: data}));
		});
	};

	$scope.addRollup = function (task, rollup, edit) { // todo rename
		var modalInstance = $uibModal.open({
			templateUrl: 'rollup-create.html?cacheBust=' + Math.random().toString(36).slice(2), //keep dialog from caching
			controller: 'KairosDBTargetCtrl',
			backdrop: 'static', // disable closing of dialog with click away
			keyboard: false, // disable closing dialog with ESC
			resolve: {
				rollupTaskId: function () {
					return task.id;
				},
				rollup: function () {
					return rollup;
				}
			}
		});

		modalInstance.result.then(
			function (newRollup) {
				if (edit) {
					task.rollups.splice(task.rollups.indexOf(rollup), 1);
				}
				task.rollups.push(newRollup);
				$scope.saveTask(task);
			});
	};

	$scope.editRollup = function (task, rollup) {
		$scope.addRollup(task, rollup, true);
	};

	$scope.saveRollup = function (task, rollup) {
		rollup.edit = false;
	};

	$scope.deleteRollup = function (task, rollup) {
		for (var i = 0; i < task.rollups.length; i++) {
			if (task.rollups[i].id == rollup.id)
				task.rollups.splice(i, 1);
		}
		$scope.saveRollupTask(task);
	};

	$scope.postNewRollupTask = function (task) {
		// todo need to remove "edit" property

		var res = $http.post('/api/v1/rollups/task', task); // todo don't hardcode?
		res.success(function (data, status, headers, config) {
			//$scope.message = data;
			console.log(status);
		});
		res.error(function (data, status, headers, config) {
			alert("failure message: " + JSON.stringify({data: data}));
		});
	};

	$scope.saveTask = function (task) {
		// todo need to remove "edit" property

		var res = $http.post('/api/v1/rollups/rollup', task); // todo don't hardcode?
		res.success(function (data, status, headers, config) {
			//$scope.message = data;
			console.log(status);
		});
		res.error(function (data, status, headers, config) {
			alert("failure message: " + JSON.stringify({data: data}));
		});
	};

});