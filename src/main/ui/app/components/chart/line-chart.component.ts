import {Component, ElementRef, Input, OnChanges, SimpleChange} from '@angular/core';
import * as moment from 'moment';
import * as numeral from 'numeral';

@Component({
    selector: 'kairos-linechart',
    templateUrl: './line-chart.component.html',
    styleUrls: ['./line-chart.component.css']
})
export class LineChartComponent implements OnChanges {

    @Input()
    public queryResult: {}[];

    public hideResetZoom: boolean;


    public constructor(private rootNode: ElementRef) {
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if (changes['queryResult'] && this.queryResult !== undefined) {
            this.showChart(this.queryResult, null);
            this.hideResetZoom = true;
        }

    }

    showChart(queries, metricData) {
        if (queries.length == 0) {
            return;
        }

        let yaxis = [];
        var dataPointCount = 0;
        var data = [];
        var axisCount = 0;
        var metricCount = 0;
        queries.forEach(function (resultSet) {
            var axis = {};
            if (metricCount == 0) {
                yaxis.push(axis);
                axisCount++;
            }
            else if ((metricData != null) && (metricData[metricCount].scale)) {
                axis['position'] = 'right'; // Flot
                yaxis.push(axis);
                axisCount++;
            }

            resultSet.results.forEach(function (queryResult) {

                var groupByMessage = "";
                var groupBy = queryResult.group_by;
                var groupType;
                //debugger;
                if (groupBy) {
                    $.each(groupBy, function (index, group) {
                        if (group.name == 'type') {
                            groupType = group.type;
                            return;
                        }

                        groupByMessage += '<br>(' + group.name + ': ';

                        var first = true;
                        $.each(group.group, function (key, value) {
                            if (value.length > 0) {
                                if (!first)
                                groupByMessage += ", ";
                                groupByMessage += key + '=' + value;
                                first = false;
                            }
                        });

                        groupByMessage += ')';

                    });
                }


                var result = {};
                result['name'] = queryResult.name + groupByMessage;
                result['label'] = queryResult.name + groupByMessage;
                result['data'] = queryResult.values;
                result['yaxis'] = axisCount; // Flot
                result['yAxis'] = axisCount - 1; // Highcharts

                dataPointCount += queryResult.values.length;
                data.push(result);
            });
            metricCount++;
        });

        var $status = $('#status');
        if (dataPointCount > 20000) {
            var response = confirm("You are attempting to plot more than 20,000 data points.\nThis may take a long time." +
            "\nYou may want to down sample your data.\n\nDo you want to continue?");
            if (response != true) {
                $status.html("Plotting canceled");
                return;
            }
        }

        this.showFlotChart(yaxis, data);
        $status.html("");
    }

    showFlotChart(yaxis, data) {
        var flotOptions = {
            series: {
                lines: {
                    show: true
                },
                points: {
                    show: true
                }
            },
            grid: {
                hoverable: true
            },
            selection: {
                mode: "xy"
            },
            xaxis: {
                mode: "time",
                timezone: "browser"
            },
            legend: {
                container: $("#graphLegend"),
                noColumns: 5
            },
            colors: ["#4572a7", "#aa4643", "#89a54e", "#80699b", "#db843d"]
        };

        flotOptions['yaxes'] = yaxis;

        setTimeout(() => {
            this.drawSingleSeriesChart(data, flotOptions);
        }, 0);

        $("#resetZoom").click(() => {
            this.hideResetZoom = true;
            this.drawSingleSeriesChart(data, flotOptions);
        });
    }

    drawSingleSeriesChart( data, flotOptions) {
        var $chartContainer = $("#chartContainer");

        $.plot($chartContainer, data, flotOptions);
        let previousPoint = null;

        let onPlotHover: any = (event, pos, item) => {
            if (item) {
                if (previousPoint != item.dataIndex) {
                    previousPoint = item.dataIndex;

                    $("#tooltip").remove();
                    var x = item.datapoint[0];
                    var y = item.datapoint[1].toFixed(2);
                    /*
                    var timestamp = new Date(x);
                    var formattedDate = $.plot.formatDate(timestamp, "%b %e, %Y %H:%M:%S.millis %p");
                    formattedDate = formattedDate.replace("millis", timestamp.getMilliseconds());
                    formattedDate += " " + this.getTimezone(timestamp);
                    */
                    var formattedDate = moment(x).format("MMM D, YYYY h:mm:ss.SSS z")
                    var numberFormat = (y % 1 != 0) ? '0,0[.00]' : '0,0';
                    this.showTooltip(item.pageX, item.pageY,
                        item.series.label + "<br>" + formattedDate + "<br>" + numeral(y).format(numberFormat));
                    }
                } else {
                    $("#tooltip").remove();
                    previousPoint = null;
                }
            };

            $chartContainer.bind("plothover", onPlotHover);

            let onPlotSelected: any = (event, ranges) => {
                if (flotOptions.yaxes.length != (Object.keys(ranges).length - 1))
                return;

                var axes = {};
                axes['yaxes'] = [];

                $.each(ranges, function (key, value) {
                    if (key == "xaxis") {
                        axes['xaxis'] = {};
                        axes['xaxis'].min = value.from;
                        axes['xaxis'].max = value.to;
                    }
                    else {
                        var axis = {};
                        axis['min'] = value.from;
                        axis['max'] = value.to;
                        axes['yaxes'].push(axis);
                    }
                });

                $.plot($chartContainer, data, $.extend(true, {}, flotOptions, axes));
                this.hideResetZoom = false;
            }

            $chartContainer.bind("plotselected", onPlotSelected);
        }

        showTooltip(x, y, contents) {
            var tooltip = $('<div id="tooltip" class="graphTooltip">' + contents + '</div>');
            tooltip.css("position", "absolute");
            tooltip.css("display", "none");
            tooltip.css("border", "2px solid #4572a7");
            tooltip.css("padding", "2px");
            tooltip.css("background-color", "#ffffff");
            tooltip.css("font-size", "14px");
            tooltip.css("font-weight", "600");
            tooltip.css("font-family", "MS, serif");
            tooltip.css("opacity", "0.80");

            tooltip.appendTo("body");

            var $body = $('body');
            var left = x + 5;
            var top = y + 5;

            // If off screen move back on screen
            if ((left) < 0)
            left = 0;
            if (left + tooltip.outerWidth() > $body.width())
            left = $body.width() - tooltip.outerWidth();
            if (top + tooltip.height() > $body.height())
            top = $body.height() - tooltip.outerHeight();

            // If now over the cursor move out of the way - causes flashing of the tooltip otherwise
            if ((x > left && x < (left + tooltip.outerWidth())) || (y < top && y > top + tooltip.outerHeight())) {
                top = y - 5 - tooltip.outerHeight(); // move up
            }

            tooltip.css("left", left);
            tooltip.css("top", top);

            tooltip.fadeIn(200);

        }

    }
