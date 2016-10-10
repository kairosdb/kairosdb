define([
		'angular',
		'app',
		'lodash',
		'kbn',
		'jquery',
		'jquery.flot',
		'jquery.flot.time',
	],
	function (angular, app, _, kbn, $) {
		'use strict';

		var module = angular.module('grafana.panels.graph');

		module.directive('graphLegend', function (popoverSrv) {

			return {
				link: function (scope, elem) {
					var $container = $('<section class="graph-legend"></section>');
					var firstRender = true;
					var panel = scope.panel;
					var data;
					var seriesList;
					var i;

					scope.$on('render', function () {
						data = scope.seriesList;
						if (data) {
							render();
						}
					});

					function getSeriesIndexForElement(el) {
						return el.parents('[data-series-index]').data('series-index');
					}

					function openColorSelector(e) {
						var el = $(e.currentTarget);
						var index = getSeriesIndexForElement(el);
						var seriesInfo = seriesList[index];
						var popoverScope = scope.$new();
						popoverScope.series = seriesInfo;
						popoverSrv.show({
							element: $(':first-child', el),
							templateUrl: 'app/panels/graph/legend.popover.html',
							scope: popoverScope
						});
					}

					function toggleSeries(e) {
						var el = $(e.currentTarget);
						var index = getSeriesIndexForElement(el);
						var seriesInfo = seriesList[index];
						scope.toggleSeries(seriesInfo, e);
					}

					function sortLegend(e) {
						var el = $(e.currentTarget);
						var stat = el.data('stat');

						if (stat !== panel.legend.sort) {
							panel.legend.sortDesc = null;
						}

						// if already sort ascending, disable sorting
						if (panel.legend.sortDesc === false) {
							panel.legend.sort = null;
							panel.legend.sortDesc = null;
							render();
							return;
						}

						panel.legend.sortDesc = !panel.legend.sortDesc;
						panel.legend.sort = stat;
						render();
					}

					function getTableHeaderHtml(statName) {
						if (!panel.legend[statName]) {
							return "";
						}
						var html = '<th class="pointer" data-stat="' + statName + '">' + statName;

						if (panel.legend.sort === statName) {
							var cssClass = panel.legend.sortDesc ? 'icon-caret-down' : 'icon-caret-up';
							html += ' <span class="' + cssClass + '"></span>';
						}

						return html + '</th>';
					}

					function render() {
						if (firstRender) {
							elem.append($container);
							$container.on('click', '.graph-legend-icon', openColorSelector);
							$container.on('click', '.graph-legend-alias', toggleSeries);
							$container.on('click', 'th', sortLegend);
							firstRender = false;
						}

						seriesList = data;

						$container.empty();

						$container.toggleClass('graph-legend-table', panel.legend.alignAsTable === true);

						if (panel.legend.alignAsTable) {
							var header = '<tr>';
							header += '<th colspan="2" style="text-align:left"></th>';
							if (panel.legend.values) {
								header += getTableHeaderHtml('min');
								header += getTableHeaderHtml('max');
								header += getTableHeaderHtml('avg');
								header += getTableHeaderHtml('current');
								header += getTableHeaderHtml('total');
							}
							header += '</tr>';
							$container.append($(header));
						}

						if (panel.legend.sort) {
							seriesList = _.sortBy(seriesList, function (series) {
								return series.stats[panel.legend.sort];
							});
							if (panel.legend.sortDesc) {
								seriesList = seriesList.reverse();
							}
						}

						for (i = 0; i < seriesList.length; i++) {
							var series = seriesList[i];

							// ignore empty series
							if (panel.legend.hideEmpty && series.allIsNull) {
								continue;
							}

							var html = '<div class="graph-legend-series';
							if (series.yaxis === 2) {
								html += ' pull-right';
							}
							if (scope.hiddenSeries[series.alias]) {
								html += ' graph-legend-series-hidden';
							}
							html += '" data-series-index="' + i + '">';
							html += '<div class="graph-legend-icon">';
							html += '<i class="icon-minus pointer" style="color:' + series.color + '"></i>';
							html += '</div>';

							html += '<div class="graph-legend-alias">';
							html += '<a>' + series.label + '</a>';
							html += '</div>';

							var avg = series.formatValue(series.stats.avg);
							var current = series.formatValue(series.stats.current);
							var min = series.formatValue(series.stats.min);
							var max = series.formatValue(series.stats.max);
							var total = series.formatValue(series.stats.total);

							if (panel.legend.values) {
								if (panel.legend.min) {
									html += '<div class="graph-legend-value min">' + min + '</div>';
								}
								if (panel.legend.max) {
									html += '<div class="graph-legend-value max">' + max + '</div>';
								}
								if (panel.legend.avg) {
									html += '<div class="graph-legend-value avg">' + avg + '</div>';
								}
								if (panel.legend.current) {
									html += '<div class="graph-legend-value current">' + current + '</div>';
								}
								if (panel.legend.total) {
									html += '<div class="graph-legend-value total">' + total + '</div>';
								}
							}

							html += '</div>';
							$container.append($(html));
						}
					}
				}
			};
		});

	});
