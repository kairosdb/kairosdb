function updateChart() {
	$("#errorContainer").hide();

	var query = new pulse.MetricQuery();

	// todo cachetime

	$('.metricContainer').each(function (index, element) {
		var $metricContainer = $(element);
		var metricName = $metricContainer.find('.metricName').val();
		if (!metricName) {
			showErrorMessage("Metric Name is required.");
			return;
		}

		var groupBy = $metricContainer.find('.metricGroupBy').val();
		var metric = new pulse.Metric(metricName, groupBy);

		// Add aggregators
		$metricContainer.find(".aggregator").each(function (index, aggregator) {
			var name = $(aggregator).find(".aggregatorName").val();
			var value = $(aggregator).find(".aggregatorSamplingValue").val();
			var unit = $(aggregator).find(".aggregatorSamplingUnit").val();

			metric.addAggregator(name, value, unit);
		});

		// Add Tags
		$metricContainer.find("[name='tags']").each(function (index, tagContainer) {
			var name = $(tagContainer).find("[name='tagName']").val();
			var value = $(tagContainer).find("[name='tagValue']").val();

			if (name && value)
				metric.addTag(name, value);
		});

		query.addMetric(metric);
	});

	var startTimeAbsolute = $("#startTime").datepicker("getDate");
	var startTimeRelativeValue = $("#startRelativeValue").val();

	if (startTimeAbsolute != null) {
		query.setStartAbsolute(startTimeAbsolute.getTime());
	}
	else if (startTimeRelativeValue) {
		query.setStartRelative(startTimeRelativeValue, $("#startRelativeUnit").val())
	}
	else {
		showErrorMessage("Start time is required.");
		return;
	}

	var endTimeAbsolute = $("#endTime").datepicker("getDate");
	if (endTimeAbsolute != null) {
		query.setEndAbsolute(endTimeAbsolute.getTime());
	}
	else {
		var endRelativeValue = $("#endRelativeValue").val();
		if (endRelativeValue) {
			query.setEndRelative(endRelativeValue, $("#endRelativeUnit").val())
		}
	}

	$("#query-text").val(JSON.stringify(query, null, 2));
	showChartForQuery("", "(Click and drag to zoom)", "", query);
}

function showErrorMessage(message) {
	var $errorContainer = $("#errorContainer");
	$errorContainer.show();
	$errorContainer.html("");
	$errorContainer.append(message);
}

function removeMetric(removeButton) {
	if (metricCount == 0) {
		return;
	}

	var count = removeButton.data("metricCount");
	$('#metricContainer' + count).remove();
	$('#metricTab' + count).remove();
	$("#tabs").tabs("refresh");
}

var metricCount = -1;

function addMetric() {
	metricCount += 1;

	// Create tab
	var $newMetric = $('<li id="metricTab' + metricCount + '">' +
		'<a class="metricTab" style="padding-right:2px;" href="#metricContainer' + metricCount + '">Metric</a>' +
		'<button id="removeMetric' + metricCount + '" style="background:none; border: none; width:15px;"></button></li>');
	$newMetric.appendTo('#tabs .ui-tabs-nav');

	var removeButton = $('#removeMetric' + metricCount);
	removeButton.data("metricCount", metricCount);

	// Add remove metric button
	removeButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-close'
		}
	}).click(function () {
			removeMetric(removeButton);
		});

	// Add tab content
	var tagContainerName = "metric-" + metricCount + "-tagsContainer";
	var $metricContainer = $("#metricTemplate").clone();
	$metricContainer
		.attr('id', 'metricContainer' + metricCount)
		.addClass("metricContainer")
		.appendTo('#tabs');

	// Add text listener to name
	var $tab = $newMetric.find('.metricTab');
	$metricContainer.find(".metricName").bind("change paste keyup autocompleteclose", function () {
		var metricName = $(this).val();
		if (metricName.length > 0) {
			$tab.text(metricName);
		}
		else {
			$tab.text("metric");
		}
	});

	addAutocomplete($metricContainer);

	// Setup tag button
	var tagButtonName = "mertric-" + metricCount + "AddTagButton";
	var tagButton = $metricContainer.find("#tagButton");
	tagButton.attr("id", tagButtonName);
	tagButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-plus'
		}
	}).click(function () {
			addTag(tagContainer)
		});

	// Add new tag
	var tagContainer = $('<div id="' + tagContainerName + '"></div>');
	tagContainer.appendTo($metricContainer);
	addTag(tagContainer);

	// Rename Aggregator Container
	$metricContainer.find("#aggregatorContainer").attr('id', 'metric-' + metricCount + 'AggregatorContainer');
	var $aggregatorContainer = $('#metric-' + metricCount + 'AggregatorContainer');

	// Listen to aggregator button
	var aggregatorButton = $metricContainer.find("#addAggregatorButton");
	aggregatorButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-plus'
		}
	}).click(function () {
			addAggregator($aggregatorContainer)
		});

	// Add a default aggregator
	addAggregator($aggregatorContainer);

	// Tell tabs object to update changes
	$("#tabs").tabs("refresh");

	// Activate newly added tab
	var lastTab = $(".ui-tabs-nav").children().size() - 1;
	$("#tabs").tabs({active: lastTab});
}

function addAggregator(container) {
	var aggregators = container.find(".aggregator");

	if (aggregators.length > 0) {
		// Add arrow
		$('<span class="ui-icon ui-icon-arrowthick-1-s" style="margin-left: 45px;"></span>').appendTo(container);
	}

	var $aggregatorContainer = $("#aggregatorTemplate").clone();
	$aggregatorContainer.removeAttr("id").appendTo(container);
	$aggregatorContainer.show();

	// Add listener for aggregator change
	container.find(".aggregatorName").change(function () {
		var name = container.find(".aggregatorName").val();
		if (name == "sort" || name == "rate") {
			container.find(".aggregatorSampling").hide();

			// clear values
			container.find(".aggregatorSamplingValue").val("")
		}
		else
			container.find(".aggregatorSampling").show();
	});
}

function addAutocomplete(metricContainer) {
	metricContainer.find(".metricName")
		.autocomplete({
			source: metricNames
		});

	metricContainer.find("[name='tagName']")
		.autocomplete({
			source: tagNames
		});

	metricContainer.find(".metricGroupBy")
		.autocomplete({
			source: tagNames
		});

	metricContainer.find("[name='tagValue']")
		.autocomplete({
			source: tagValues
		});
}

function addTag(tagContainer) {

	var newDiv = $("<div></div>");
	tagContainer.append(newDiv);
	$("#tagContainer").clone().removeAttr("id").appendTo(newDiv);

	// add auto complete
	newDiv.find("[name='tagName']").autocomplete({
		source: tagNames
	});

	// add auto complete
	newDiv.find("[name='tagValue']").autocomplete({
		source: tagValues
	});
}

function showChartForQuery(title, subTitle, yAxisTitle, query) {
	pulse.dataPointsQuery(query, function (queries) {
		showChart(title, subTitle, yAxisTitle, query, queries);
	});
}

function showChart(title, subTitle, yAxisTitle, query, queries) {
	if (queries.length == 0) {
		return;
	}

	var data = [];
	queries.forEach(function (resultSet) {

		var metricCount = 0;
		resultSet.results.forEach(function (queryResult) {

			var groupByMessage = "";
			var groupBy = query.metrics[metricCount].group_by;
			if (groupBy)
				groupByMessage = '<br>(' + groupBy + '=' + queryResult.tags[groupBy][0] + ')';

			var result = {};
			result.name = queryResult.name + groupByMessage;
			result.label = queryResult.name + groupByMessage;
			result.data = queryResult.values;
			data.push(result);
		});
	});

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
			mode: "x"
		},
		xaxis: {
			mode: "time"
		},
		legend: {
			container: $("#graphLegend"),
			noColumns: 5
		},
		colors: ["#4572a7", "#aa4643", "#89a54e", "#80699b", "#db843d"]
	};

	drawSingleSeriesChart(title, subTitle, yAxisTitle, data, flotOptions);

	$("#resetZoom").click(function () {
		drawSingleSeriesChart(title, subTitle, yAxisTitle, data, flotOptions);
		$("#resetZoom").hide();
	});
}

function drawSingleSeriesChart(title, subTitle, yAxisTitle, data, flotOptions) {
	$("#flotTitle").html(subTitle);

	var $flotcontainer = $("#flotcontainer");

	$.plot($flotcontainer, data, flotOptions);

	$flotcontainer.bind("plothover", function (event, pos, item) {
		if (item) {
			if (previousPoint != item.dataIndex) {
				previousPoint = item.dataIndex;

				$("#tooltip").remove();
				var x = item.datapoint[0].toFixed(2);
				var y = item.datapoint[1].toFixed(2);

				showTooltip(item.pageX, item.pageY,
					item.series.label + "<br>" + x + " : " + y);
			}
		} else {
			$("#tooltip").remove();
			previousPoint = null;
		}
	});

	$flotcontainer.bind("plotselected", function (event, ranges) {
		$.plot($flotcontainer, data, $.extend(true, {}, flotOptions, {
			xaxis: {
				min: ranges.xaxis.from,
				max: ranges.xaxis.to
			}
		}));
		$("#resetZoom").show();
	});
}

function showTooltip(x, y, contents) {
	$('<div id="tooltip" class="graphTooltip">' + contents + '</div>').css({
		top: y + 5,
		left: x + 5
	}).appendTo("body").fadeIn(200);
}
