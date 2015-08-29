var app = angular.module('myApp', []);
app.controller('rollupController', function ($scope, $http) {
	$http.get("http://localhost:8080/api/v1/rollups/rollup") //todo don't hard code
		.success(function (response) {
			if (response)
				$scope.rollups = response;
			else
				$scope.rollups = [];

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

	$scope.scheduleModified = function (rollup, item) {
		rollup.schedule = item;
	};

	$scope.startTimeModified = function (rollup, item) {
		rollup.rollups[0].query.start_relative = item;
	};

	$scope.aggregatorModified = function (rollup, item) {
		rollup.rollups[0].query.metrics[0].aggregators[0].name = item;
	};

	$scope.aggregatorSamplingModified = function (rollup, item) {
		rollup.rollups[0].query.metrics[0].aggregators[0].sampling = item;
	};

	$scope.addRollup = function () {
		var newRollup = {
			schedule: '0 * * * * ?',
			rollups: [{
				query: {
					start_relative: {value: 1, unit: 'hours'},
					metrics: [{
						aggregators: [{
							name: "sum",
							sampling: {value: 1, unit: 'minutes'}
						}]
					}]
				}
			}]
		};
		newRollup.edit = true;
		$scope.rollups.push(newRollup);
	};

	$scope.editRollup = function (rollup) {
		rollup.edit = true;
	};

	$scope.saveRollup = function (rollup) {
		$scope.postRollup(rollup);
		rollup.edit = false;
	};

	$scope.postRollup = function (rollup) {
		// todo need to remove "edit" property

		var res = $http.post('/api/v1/rollups/rollup', rollup); // todo don't hardcode?
		res.success(function (data, status, headers, config) {
			//$scope.message = data;
			console.log(status);
		});
		res.error(function (data, status, headers, config) {
			alert("failure message: " + JSON.stringify({data: data}));
		});
	};

	$scope.$watch('passw1', function () {
		$scope.test();
	});
	$scope.$watch('passw2', function () {
		$scope.test();
	});
	$scope.$watch('fName', function () {
		$scope.test();
	});
	$scope.$watch('lName', function () {
		$scope.test();
	});

	$scope.test = function () {
		if ($scope.passw1 !== $scope.passw2) {
			$scope.error = true;
		} else {
			$scope.error = false;
		}
		$scope.incomplete = false;
		if ($scope.edit && (!$scope.fName.length || !$scope.lName.length || !$scope.passw1.length || !$scope.passw2.length)) {
			$scope.incomplete = true;
		}
	};

});