define([
		'angular',
		'app',
		'lodash',
		'jquery',
	],
	function (angular, app, _, $) {
		'use strict';

		angular
			.module('grafana.directives')
			.directive('graphiteSegment', function ($compile, $sce) {
				var inputTemplate = '<input type="text" data-provide="typeahead" ' +
					' class="grafana-target-text-input input-medium"' +
					' spellcheck="false" style="display:none"></input>';

				var buttonTemplate = '<a class="grafana-target-segment" tabindex="1" focus-me="segment.focus" ng-bind-html="segment.html"></a>';

				return {
					link: function ($scope, elem) {
						var $input = $(inputTemplate);
						var $button = $(buttonTemplate);
						var segment = $scope.segment;
						var options = null;
						var cancelBlur = null;

						$input.appendTo(elem);
						$button.appendTo(elem);

						$scope.updateVariableValue = function (value) {
							if (value === '' || segment.value === value) {
								return;
							}

							$scope.$apply(function () {
								var selected = _.findWhere($scope.altSegments, {value: value});
								if (selected) {
									segment.value = selected.value;
									segment.html = selected.html;
									segment.expandable = selected.expandable;
								}
								else {
									segment.value = value;
									segment.html = $sce.trustAsHtml(value);
									segment.expandable = true;
								}
								$scope.segmentValueChanged(segment, $scope.$index);
							});
						};

						$scope.switchToLink = function (now) {
							if (now === true || cancelBlur) {
								clearTimeout(cancelBlur);
								cancelBlur = null;
								$input.hide();
								$button.show();
								$scope.updateVariableValue($input.val());
							}
							else {
								// need to have long delay because the blur
								// happens long before the click event on the typeahead options
								cancelBlur = setTimeout($scope.switchToLink, 350);
							}
						};

						$scope.source = function (query, callback) {
							if (options) {
								return options;
							}

							$scope.$apply(function () {
								$scope.getAltSegments($scope.$index).then(function () {
									options = _.map($scope.altSegments, function (alt) {
										return alt.value;
									});

									// add custom values
									if (segment.value !== 'select metric' && _.indexOf(options, segment.value) === -1) {
										options.unshift(segment.value);
									}

									callback(options);
								});
							});
						};

						$scope.updater = function (value) {
							if (value === segment.value) {
								clearTimeout(cancelBlur);
								$input.focus();
								return value;
							}

							$input.val(value);
							$scope.switchToLink(true);

							return value;
						};

						$input.attr('data-provide', 'typeahead');
						$input.typeahead({
							source: $scope.source,
							minLength: 0,
							items: 10000,
							updater: $scope.updater
						});

						var typeahead = $input.data('typeahead');
						typeahead.lookup = function () {
							this.query = this.$element.val() || '';
							var items = this.source(this.query, $.proxy(this.process, this));
							return items ? this.process(items) : items;
						};

						$button.keydown(function (evt) {
							// trigger typeahead on down arrow or enter key
							if (evt.keyCode === 40 || evt.keyCode === 13) {
								$button.click();
							}
						});

						$button.click(function () {
							options = null;
							$input.css('width', ($button.width() + 16) + 'px');

							$button.hide();
							$input.show();
							$input.focus();

							var typeahead = $input.data('typeahead');
							if (typeahead) {
								$input.val('');
								typeahead.lookup();
							}
						});

						$input.blur($scope.switchToLink);

						$compile(elem.contents())($scope);
					}
				};
			});
	});
