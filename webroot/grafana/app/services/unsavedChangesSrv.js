define([
		'angular',
		'lodash',
		'config',
	],
	function (angular, _, config) {
		'use strict';

		if (!config.unsaved_changes_warning) {
			return;
		}

		var module = angular.module('grafana.services');

		module.service('unsavedChangesSrv', function ($rootScope, $modal, $q, $location, $timeout) {

			var self = this;
			var modalScope = $rootScope.$new();

			$rootScope.$on("dashboard-loaded", function (event, newDashboard) {
				// wait for different services to patch the dashboard (missing properties)
				$timeout(function () {
					self.original = angular.copy(newDashboard);
					self.current = newDashboard;
				}, 1200);
			});

			$rootScope.$on("dashboard-saved", function (event, savedDashboard) {
				self.original = angular.copy(savedDashboard);
				self.current = savedDashboard;
				self.orignalPath = $location.path();
			});

			$rootScope.$on("$routeChangeSuccess", function () {
				self.original = null;
				self.originalPath = $location.path();
			});

			window.onbeforeunload = function () {
				if (self.has_unsaved_changes()) {
					return "There are unsaved changes to this dashboard";
				}
			};

			this.init = function () {
				$rootScope.$on("$locationChangeStart", function (event, next) {
					if (self.originalPath === $location.path()) {
						return;
					}

					if (self.has_unsaved_changes()) {
						event.preventDefault();
						self.next = next;

						$timeout(self.open_modal);
					}
				});
			};

			this.open_modal = function () {
				var confirmModal = $modal({
					template: './app/partials/unsaved-changes.html',
					persist: true,
					show: false,
					scope: modalScope,
					keyboard: false
				});

				$q.when(confirmModal).then(function (modalEl) {
					modalEl.modal('show');
				});
			};

			this.has_unsaved_changes = function () {
				if (!self.original) {
					return false;
				}

				var current = angular.copy(self.current);
				var original = self.original;

				// ignore timespan changes
				current.time = original.time = {};
				current.refresh = original.refresh;

				// ignore template variable values
				_.each(current.templating.list, function (value, index) {
					value.current = null;
					value.options = null;
					original.templating.list[index].current = null;
					original.templating.list[index].options = null;
				});

				var currentTimepicker = _.findWhere(current.nav, {type: 'timepicker'});
				var originalTimepicker = _.findWhere(original.nav, {type: 'timepicker'});

				if (currentTimepicker && originalTimepicker) {
					currentTimepicker.now = originalTimepicker.now;
				}

				var currentJson = angular.toJson(current);
				var originalJson = angular.toJson(original);

				if (currentJson !== originalJson) {
					return true;
				}

				return false;
			};

			this.goto_next = function () {
				var baseLen = $location.absUrl().length - $location.url().length;
				var nextUrl = self.next.substring(baseLen);
				$location.url(nextUrl);
			};

			modalScope.ignore = function () {
				self.original = null;
				self.goto_next();
			};

			modalScope.save = function () {
				var unregister = $rootScope.$on('dashboard-saved', function () {
					self.goto_next();
				});

				$timeout(unregister, 2000);

				$rootScope.$emit('save-dashboard');
			};

		}).run(function (unsavedChangesSrv) {
			unsavedChangesSrv.init();
		});
	});
