// todo Add "Last Exectuted", "When to execute next", Pause button, and maybe "create by | or owned by team"

var ROLLUP_URL = "/api/v1/rollups/";

var module = angular.module('rollupApp', ['mgcrea.ngStrap',
	'mgcrea.ngStrap.tooltip', 'ui.bootstrap.modal', 'template/modal/backdrop.html',
	'template/modal/window.html']);

module.controller('rollupController', function ($scope, $http, $uibModal, orderByFilter) {

	$scope.lastSaved = null;
	$scope.tasks = [];
	$scope.taskCopies = [];

	$http.get(ROLLUP_URL)
		.success(function (response) {

			if (response) {
				$scope.tasks = response;
				$scope.taskCopies = angular.copy($scope.tasks);
				$scope.tasks = orderByFilter($scope.tasks, "name");
			}
			else
				$scope.tasks = [];
		})
		.error(function (data, status, headers, config) {
			$scope.alert("Could not read list of roll-ups from server.", status, data);
		});

	$scope.isUnchanged = function (task) {
		for (var i = 0; i < $scope.taskCopies.length; i++) {
			var original = $scope.taskCopies[i];
			if (task.id == original.id) {
				return angular.equals(task, original);
			}
		}
		return false; // New task
	};

	$scope.updateCopy = function (task) {
		for (var i = 0; i < $scope.taskCopies.length; i++) {
			var original = $scope.taskCopies[i];
			if (task.id == original.id) {
				$scope.taskCopies[i] = angular.copy(task);
				break;
			}
		}
	};

	$scope.onBlur = function (task) {
		$scope.errors = validate(task);

		if (!$scope.hasErrors()) {
			$scope.tasks = orderByFilter($scope.tasks, "name");

			if ($scope.isUnchanged(task))
				return;

			$scope.saveRollupTask(task);
		}
	};

	$scope.toHumanReadableTimeUnit = function (timeUnit) {
		if (timeUnit.value == 1)
			return timeUnit.value + " " + timeUnit.unit.substring(0, timeUnit.unit.length - 1);
		else
			return timeUnit.value + " " + timeUnit.unit;
	};

	$scope.executionUnits = ["Seconds", "Minutes", "Hours", "Weeks", "Months"];

	$scope.toTql = function (query) {
		tql = "SELECT " + query.query.metrics[0].name;
		tql += " FROM " + $scope.toHumanReadableTimeUnit(query.query.start_relative) + " to now";

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

		// Group By
		var groupBy = query.query.metrics[0].group_by;
		if (groupBy && groupBy.length > 0) {
			var groupExists = false;
			tql += " GROUP BY";
			_.each(groupBy, function (group) {
				if (groupExists) {
					tql += " AND";
				}

				if (group.name == "tag") {
					groupExists = true;
					tql += " tags(";
					_.each(group.tags, function (tag) {
						tql += '"' + tag + '", ';
					});
					tql = tql.substring(0, tql.length - 2); // Remove trailing comma and space
					tql += ")";
				}
				if (group.name == "time") {
					groupExists = true;
					tql += " time(" + $scope.toHumanReadableTimeUnit(group.range_size) + ", ";
					tql += group.group_count;
					tql += ")";
				}
				if (group.name == "value") {
					groupExists = true;
					tql += " value(" + group.range_size + ")";
				}
			});
		}

		// Save As
		tql += " SAVE AS " + query.save_as;

		return tql;
	};

	$scope.toHumanReadableAggregator = function (aggregator) {
		var result = aggregator.name + '(';
		if (aggregator.sampling) {
			result += $scope.toHumanReadableTimeUnit(aggregator.sampling);
			result += aggregator.align_start_time ? ", align-start" : "";
			result += aggregator.align_sampling ? ", align-sampling" : "";
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

	$scope.scheduleModified = function (task, unit) {
		task.executionUnit = unit;
		$scope.onBlur(task);
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
		var task = {executionValue: 1, executionUnit: "Hours", rollups: []};
		task.edit = true;
		$scope.tasks.push(task);

	};

	$scope.editRollupTask = function (task) {
		task.edit = true;
	};

	$scope.saveRollupTask = function (task) {
		$scope.saveTask(task);
		task.edit = false;
	};

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

	$scope.addRollup = function (task, rollup, edit) {
		var modalInstance = $uibModal.open({
			templateUrl: 'rollup-create.html?cacheBust=' + Math.random().toString(36).slice(2), //keep dialog from caching
			controller: 'CreateController',
			size: 'lg',
			backdrop: 'static', // disable closing of dialog with click away
			keyboard: false, // disable closing dialog with ESC
			resolve: {
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

	$scope.pasteQuery = function (task, rollup, edit) {
		var modalInstance = $uibModal.open({
			templateUrl: 'paste-query.html?cacheBust=' + Math.random().toString(36).slice(2), //keep dialog from caching
			controller: 'PasteQueryCtrl',
			size: 'lg',
			backdrop: 'static', // disable closing of dialog with click away
			keyboard: false, // disable closing dialog with ESC
			resolve: {
				rollup: function () {
					return rollup;
				}
			}
		});

		modalInstance.result.then(
			function (newRollup) {
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
		bootbox.confirm({
			size: 'medium',
			message: "Are you sure you want to delete the query?",
			callback: function (result) {
				if (result) {
					task.rollups.splice(task.rollups.indexOf(rollup), 1);
					$scope.saveRollupTask(task);
				}
			}
		});
	};

	$scope.saveTask = function (task) {
		// todo remove these properties from task: executionUnit, edit, executionValue

		task.execution_interval = {
			value: task.executionValue,
			unit: task.executionUnit
		};

		var res = $http.post(ROLLUP_URL, task);
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

	// TODO how to not duplicate in the other controller?
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

	$scope.isNumber = function (num) {
		return !isNaN(num)
	};

	//////////////////////////////
	// VALIDATION
	//////////////////////////////

	$scope.hasErrors = function () {
		return !_.isEmpty($scope.errors);
	};

	function validate(task) {
		var errs = {};

		if (!task.name || _.isEmpty(task.name)) {
			errs.name = "Name cannot be empty.";
			$scope.alert(errs.name);
		}
		if (!task.executionValue) {
			errs.executionValue = "You must specify when the roll-up will be executed.";
			$scope.alert(errs.executionValue);
		}
		if (task.executionValue && !$scope.isNumber(task.executionValue)) {
			errs.executionValue = "Must be a number.";
			$scope.alert(errs.executionValue);
		}

		return errs;
	}
});

/**
 * Set focus for dynamically create element
 */
module.directive('focus', function () {
	return function (scope, element, attr) {
		element[0].focus();
	};
});
