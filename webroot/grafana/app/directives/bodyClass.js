define([
		'angular',
		'app',
		'lodash'
	],
	function (angular) {
		'use strict';

		angular
			.module('grafana.directives')
			.directive('bodyClass', function () {
				return {
					link: function ($scope, elem) {

						var lastHideControlsVal;

						$scope.$watch('submenuEnabled', function () {
							if (!$scope.dashboard) {
								return;
							}

							elem.toggleClass('submenu-controls-visible', $scope.submenuEnabled);
						});

						$scope.$watch('dashboard.hideControls', function () {
							if (!$scope.dashboard) {
								return;
							}

							var hideControls = $scope.dashboard.hideControls || $scope.playlist_active;

							if (lastHideControlsVal !== hideControls) {
								elem.toggleClass('hide-controls', hideControls);
								lastHideControlsVal = hideControls;
							}
						});

						$scope.$watch('playlist_active', function () {
							elem.toggleClass('hide-controls', $scope.playlist_active === true);
							elem.toggleClass('playlist-active', $scope.playlist_active === true);
						});
					}
				};
			});

	});
