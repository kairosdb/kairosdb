define([
		'angular',
		'lodash',
	],
	function (angular, _) {
		'use strict';

		var module = angular.module('grafana.services');
		module.service('panelSrv', function ($rootScope, $timeout, datasourceSrv) {

			this.init = function ($scope) {
				if (!$scope.panel.span) {
					$scope.panel.span = 12;
				}

				$scope.inspector = {};

				$scope.editPanel = function () {
					if ($scope.panelMeta.fullscreen) {
						$scope.toggleFullscreen(true);
					}
					else {
						$scope.appEvent('show-dash-editor', {
							src: 'app/partials/paneleditor.html',
							scope: $scope
						});
					}
				};

				$scope.sharePanel = function () {
					$scope.appEvent('show-modal', {
						src: './app/partials/share-panel.html',
						scope: $scope.$new()
					});
				};

				$scope.editPanelJson = function () {
					$scope.appEvent('show-json-editor', {
						object: $scope.panel,
						updateHandler: $scope.replacePanel
					});
				};

				$scope.duplicatePanel = function () {
					$scope.dashboard.duplicatePanel($scope.panel, $scope.row);
				};

				$scope.updateColumnSpan = function (span) {
					$scope.panel.span = Math.min(Math.max($scope.panel.span + span, 1), 12);

					$timeout(function () {
						$scope.$emit('render');
					});
				};

				$scope.addDataQuery = function () {
					$scope.panel.targets.push({target: ''});
				};

				$scope.removeDataQuery = function (query) {
					$scope.panel.targets = _.without($scope.panel.targets, query);
					$scope.get_data();
				};

				$scope.setDatasource = function (datasource) {
					$scope.panel.datasource = datasource;
					$scope.datasource = datasourceSrv.get(datasource);

					if (!$scope.datasource) {
						$scope.panelMeta.error = "Cannot find datasource " + datasource;
						return;
					}
				};

				$scope.changeDatasource = function (datasource) {
					$scope.setDatasource(datasource);
					$scope.get_data();
				};

				$scope.toggleEditorHelp = function (index) {
					if ($scope.editorHelpIndex === index) {
						$scope.editorHelpIndex = null;
						return;
					}
					$scope.editorHelpIndex = index;
				};

				$scope.toggleFullscreen = function (edit) {
					$scope.dashboardViewState.update({
						fullscreen: true,
						edit: edit,
						panelId: $scope.panel.id
					});
				};

				$scope.otherPanelInFullscreenMode = function () {
					return $scope.dashboardViewState.fullscreen && !$scope.fullscreen;
				};

				// Post init phase
				$scope.fullscreen = false;
				$scope.editor = {index: 1};

				$scope.datasources = datasourceSrv.getMetricSources();
				$scope.setDatasource($scope.panel.datasource);
				$scope.dashboardViewState.registerPanel($scope);

				if ($scope.get_data) {
					var panel_get_data = $scope.get_data;
					$scope.get_data = function () {
						if ($scope.otherPanelInFullscreenMode()) {
							return;
						}

						delete $scope.panelMeta.error;
						$scope.panelMeta.loading = true;

						panel_get_data();
					};

					if (!$scope.skipDataOnInit) {
						$scope.get_data();
					}
				}
			};
		});

	});
