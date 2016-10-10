define([
		'angular',
		'lodash',
		'jquery'
	],
	function (angular, _, $) {
		'use strict';

		angular
			.module('grafana.directives')
			.directive('configModal', function ($modal, $q, $timeout) {
				return {
					restrict: 'A',
					link: function (scope, elem, attrs) {
						var partial = attrs.configModal;
						var id = '#' + partial.replace('.html', '').replace(/[\/|\.|:]/g, '-') + '-' + scope.$id;

						elem.bind('click', function () {
							if ($(id).length) {
								elem.attr('data-target', id).attr('data-toggle', 'modal');
								scope.$apply(function () {
									scope.$broadcast('modal-opened');
								});
								return;
							}

							var panelModal = $modal({
								template: partial,
								persist: true,
								show: false,
								scope: scope,
								keyboard: false
							});

							$q.when(panelModal).then(function (modalEl) {
								elem.attr('data-target', id).attr('data-toggle', 'modal');

								$timeout(function () {
									if (!modalEl.data('modal').isShown) {
										modalEl.modal('show');
									}
								}, 50);
							});

							scope.$apply();
						});
					}
				};
			});
	});
