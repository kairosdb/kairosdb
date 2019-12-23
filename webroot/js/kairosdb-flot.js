
/*
 * this simulates using the timezoneJS library so that the graph will show timestamps
 * in the correct time zone. since we are already loading momement.tz.js and it's data,
 * there's no point in loading an another tz data set.
 */
window.timezoneJS = {Date: Date};

/*
 * allows the graph to do zone-aware x axis timestamp display
 */
Date.prototype.strftime = function(format) {
	return moment.tz(this, this.timezone).strftime(format);
}

Date.prototype.setTimezone = function(ts) {
	this.timezone = ts;
}

function showFlotChart(subTitle, yaxis, data, timezone) {
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
			timezone: timezone
		},
		legend: {
			container: $("#graphLegend"),
			noColumns: 4,
			sorted: true
		},
		colors: ["#4572a7", "#aa4643", "#89a54e", "#80699b", "#db843d"]
	};

	flotOptions.yaxes = yaxis;

	$('#chartContainer')._addClass('flotChartSize'); // Flot requires that the chart have an explicit size

	setTimeout(function(){
		drawSingleSeriesChart(subTitle, data, flotOptions);
	}, 0);

	$("#resetZoom").click(function () {
		$("#resetZoom").hide();
		drawSingleSeriesChart(subTitle, data, flotOptions);
	});
}

function drawSingleSeriesChart(subTitle, data, flotOptions) {
	$("#flotTitle").html(subTitle);

	var $chartContainer = $("#chartContainer");

	$.plot($chartContainer, data, flotOptions);

	$chartContainer.bind("plothover", function (event, pos, item) {
		if (item) {
			if (previousPoint != item.dataIndex) {
				previousPoint = item.dataIndex;
				$("#tooltip").remove();
				var x = item.datapoint[0];
				var y = item.datapoint[1].toFixed(2);
				var numberFormat = (y % 1 != 0) ? '0,0[.000]' : '0,0';
				var timezone = item.series.xaxis.options.timezone || "utc";
				showTooltip(item.pageX, item.pageY,
				item.series.label + "<br>" + moment.tz(new Date(x), timezone).format("YYYY-MM-DD HH:mm:ss.SSS z") + "<br>" +
				numeral(y).format(numberFormat));
			}
		} else {
			$("#tooltip").remove();
			previousPoint = null;
		}
	});

	$chartContainer.bind("plotselected", function (event, ranges) {
		if (flotOptions.yaxes.length != (Object.keys(ranges).length - 1))
			return;

		var axes = {};
		axes.yaxes = [];

		$.each(ranges, function(key, value) {
			if (key == "xaxis")
			{
				axes.xaxis = {};
				axes.xaxis.min = value.from;
				axes.xaxis.max = value.to;
			}
			else {
				var axis = {};
				axis.min = value.from;
				axis.max = value.to;
				axes.yaxes.push(axis);
			}
		});

		$.plot($chartContainer, data, $.extend(true, {}, flotOptions, axes));
		$("#resetZoom").show();
	});
}

function showTooltip(x, y, contents) {
	var tooltip = $('<div id="tooltip" class="graphTooltip">' + contents + '</div>');
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

	tooltip.fadeIn(100);
}
