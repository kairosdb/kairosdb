define([
		'angular',
		'lodash'
	],
	function (angular, _) {
		'use strict';

		var module = angular.module('grafana.controllers');

		var seriesList = null;

		module.controller('InfluxTargetCtrl', function ($scope, $timeout) {

			$scope.init = function () {
				var target = $scope.target;

				target.function = target.function || 'mean';
				target.column = target.column || 'value';

				// backward compatible correction of schema
				if (target.condition_value) {
					target.condition = target.condition_key + ' ' + target.condition_op + ' ' + target.condition_value;
					delete target.condition_key;
					delete target.condition_op;
					delete target.condition_value;
				}

				if (target.groupby_field_add === false) {
					target.groupby_field = '';
					delete target.groupby_field_add;
				}

				$scope.rawQuery = false;

				$scope.functions = [
					'count', 'mean', 'sum', 'min',
					'max', 'mode', 'distinct', 'median',
					'derivative', 'stddev', 'first', 'last',
					'difference'
				];

				$scope.operators = ['=', '=~', '>', '<', '!~', '<>'];
				$scope.oldSeries = target.series;
				$scope.$on('typeahead-updated', function () {
					$timeout($scope.get_data);
				});
			};

			$scope.showQuery = function () {
				$scope.target.rawQuery = true;
			};

			$scope.hideQuery = function () {
				$scope.target.rawQuery = false;
			};

			// Cannot use typeahead and ng-change on blur at the same time
			$scope.seriesBlur = function () {
				if ($scope.oldSeries !== $scope.target.series) {
					$scope.oldSeries = $scope.target.series;
					$scope.columnList = null;
					$scope.get_data();
				}
			};

			$scope.changeFunction = function (func) {
				$scope.target.function = func;
				$scope.get_data();
			};

			// called outside of digest
			$scope.listColumns = function (query, callback) {
				if (!$scope.columnList) {
					$scope.$apply(function () {
						$scope.datasource.listColumns($scope.target.series).then(function (columns) {
							$scope.columnList = columns;
							callback(columns);
						});
					});
				}
				else {
					return $scope.columnList;
				}
			};

			$scope.listSeries = function (query, callback) {
				if (query !== '') {
					seriesList = [];
					$scope.datasource.listSeries(query).then(function (series) {
						seriesList = series;
						console.log(series);
						callback(seriesList);
					});
				}
				else {
					return seriesList;
				}
			};

			$scope.moveMetricQuery = function (fromIndex, toIndex) {
				_.move($scope.panel.targets, fromIndex, toIndex);
			};

			$scope.duplicate = function () {
				var clone = angular.copy($scope.target);
				$scope.panel.targets.push(clone);
			};

		});

	});
