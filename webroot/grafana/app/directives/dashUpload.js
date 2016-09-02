define([
		'angular',
		'kbn'
	],
	function (angular, kbn) {
		'use strict';

		var module = angular.module('grafana.directives');

		module.directive('dashUpload', function (timer, alertSrv, $location) {
			return {
				restrict: 'A',
				link: function (scope) {
					function file_selected(evt) {
						var files = evt.target.files; // FileList object
						var readerOnload = function () {
							return function (e) {
								scope.$apply(function () {
									try {
										window.grafanaImportDashboard = JSON.parse(e.target.result);
									} catch (err) {
										console.log(err);
										scope.appEvent('alert-error', ['Import failed', 'JSON -> JS Serialization failed: ' + err.message]);
										return;
									}
									var title = kbn.slugifyForUrl(window.grafanaImportDashboard.title);
									$location.path('/dashboard/import/' + title);
								});
							};
						};
						for (var i = 0, f; f = files[i]; i++) {
							var reader = new FileReader();
							reader.onload = (readerOnload)(f);
							reader.readAsText(f);
						}
					}

					// Check for the various File API support.
					if (window.File && window.FileReader && window.FileList && window.Blob) {
						// Something
						document.getElementById('dashupload').addEventListener('change', file_selected, false);
					} else {
						alertSrv.set('Oops', 'Sorry, the HTML5 File APIs are not fully supported in this browser.', 'error');
					}
				}
			};
		});
	});
