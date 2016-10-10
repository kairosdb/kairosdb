define(['angular', 'jquery', 'lodash', 'moment'], function (angular, $, _, moment) {
	'use strict';

	var module = angular.module('grafana.filters');

	module.filter('stringSort', function () {
		return function (input) {
			return input.sort();
		};
	});

	module.filter('slice', function () {
		return function (arr, start, end) {
			if (!_.isUndefined(arr)) {
				return arr.slice(start, end);
			}
		};
	});

	module.filter('stringify', function () {
		return function (arr) {
			if (_.isObject(arr) && !_.isArray(arr)) {
				return angular.toJson(arr);
			} else {
				return _.isNull(arr) ? null : arr.toString();
			}
		};
	});

	module.filter('moment', function () {
		return function (date, mode) {
			switch (mode) {
				case 'ago':
					return moment(date).fromNow();
			}
			return moment(date).fromNow();
		};
	});

	module.filter('noXml', function () {
		var noXml = function (text) {
			return _.isString(text)
				? text
				.replace(/&/g, '&amp;')
				.replace(/</g, '&lt;')
				.replace(/>/g, '&gt;')
				.replace(/'/g, '&#39;')
				.replace(/"/g, '&quot;')
				: text;
		};
		return function (text) {
			return _.isArray(text)
				? _.map(text, noXml)
				: noXml(text);
		};
	});

	module.filter('interpolateTemplateVars', function (templateSrv) {
		function interpolateTemplateVars(text) {
			return templateSrv.replaceWithText(text);
		}

		interpolateTemplateVars.$stateful = true;

		return interpolateTemplateVars;
	});

});
