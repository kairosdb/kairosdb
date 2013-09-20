function displayQuery() {
	var queryString = $('#query-hidden-text').val();
	if ($('#query-type-json').is(':checked'))
		$("#query-text").val(queryString);
	else
		$("#query-text").val('var query = ' + queryString.replace(/\"(\w*)\":/g, "$1:") + ';');
}

function updateChart() {
	$("#resetZoom").hide();
	$("#errorContainer").hide();

	$("#status").html("");
	$("#queryTime").html("");
	$("#numDataPoints").html("");

	var query = new kairosdb.MetricQuery();

	// todo cachetime

	$('.metricContainer').each(function (index, element) {
		var $metricContainer = $(element);
		var metricName = $metricContainer.find('.metricName').val();
		if (!metricName) {
			showErrorMessage("Metric Name is required.");
			return;
		}

		var metric = new kairosdb.Metric(metricName);

		$metricContainer.find(".groupBy").each(function (index, groupBy) {
			var name = $(groupBy).find(".groupByName").val();

			if (name == "tags") {
				var tags = $(groupBy).find(".groupByTagsValue").val();
				if (!tags || tags.length < 1) {
					showErrorMessage("Missing Group By tag names.");
					return true; // continue to next item
				}
				metric.addGroupBy(new kairosdb.TagGroupBy(tags));
			}
			else if (name == "time") {
				var value = $(groupBy).find(".groupByTimeSizeValue").val();
				var unit = $(groupBy).find(".groupByTimeUnit").val();
				var count = $(groupBy).find(".groupByTimeCount").val();

				if (value < 1) {
					showErrorMessage("Missing Time Group By size must be greater than 0.");
					return true;
				}

				if (count < 1) {
					showErrorMessage("Missing Time Group By count must be greater than 0.");
					return true;
				}
				metric.addGroupBy(new kairosdb.TimeGroupBy(value, unit, count));
			}
			else if (name == "value") {
				var size = $(groupBy).find(".groupByValueValue").val();
				if (size < 1) {
					showErrorMessage("Missing Value Group By size must be greater than 0.");
					return true;
				}
				metric.addGroupBy(new kairosdb.ValueGroupBy(size));
			}
		});

		// Add aggregators
		$metricContainer.find(".aggregator").each(function (index, aggregator) {
			var name = $(aggregator).find(".aggregatorName").val();

			var unit;
			if (name == 'rate') {
				unit = $(aggregator).find(".aggregatorSamplingUnit").val();
				metric.addRate(unit);
			}
			else if (name == 'histogram') {
				value = $(aggregator).find(".aggregatorSamplingValue").val();
				if (!isValidInteger(value)) {
					return true;
				}
				unit = $(aggregator).find(".aggregatorSamplingUnit").val();
				var percentile = $(aggregator).find(".aggregatorPercentileValue").val();
				if (!isValidPercentile(percentile)) {
					return true;
				}
				metric.addHistogram(value, unit, percentile);
			}
			else if (name == 'div') {
				var divisor = $(aggregator).find(".divisorValue").val();
				metric.addDivideAggregator(divisor);
			}
			else {
				var value = $(aggregator).find(".aggregatorSamplingValue").val();
				if (!isValidInteger(value)) {
					return true;
				}
				unit = $(aggregator).find(".aggregatorSamplingUnit").val();
				metric.addAggregator(name, value, unit);
			}
		});

		function isValidPercentile(percentile) {
			var intRegex = /^(0*\.\d*|(0*1(\.0*|))|0+)$/;
			if (!intRegex.test(percentile)) {
				showErrorMessage("percentile value must be between [0-1]");
				return false;
			}
			else {
				return true;
			}
		}

		function isValidInteger(value) {
			var intRegex = /^\d+$/;
			if (!intRegex.test(value)) {
				showErrorMessage("sampling value must be an integer greater than 0.");
				return false;
			}
			else {
				return true;
			}
		}

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

	var metricData = getAdditionalChartData();
	$('#query-hidden-text').val(JSON.stringify(query, null, 2));
	displayQuery();

	var $graphLink = $("#graph_link");
	$graphLink.attr("href", "view.html?q=" + encodeURI(JSON.stringify(query, null, 0)) + "&d=" + encodeURI(JSON.stringify(metricData, null, 0)));
	$graphLink.show();
	showChartForQuery("(Click and drag to zoom)", query, metricData);
}

/**
 * Returns additional data in a JSON object of the form
 * metrics: [{scale:true}, {scale:false}]
 */
function getAdditionalChartData() {
	var metricDataArray = [];

	$('.metricContainer').each(function (index, element) {
		var metric = {};
		metric.scale = $(element).find(".scale").is(':checked');
		metricDataArray.push(metric);
	});

	return metricDataArray;
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

	var tagContainer = $('<div id="' + tagContainerName + '" metricCount="' + metricCount + '"></div>');
	tagContainer.appendTo($metricContainer);

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

	// Rename GroupBy Container
	$metricContainer.find("#groupByContainer").attr('id', 'metric-' + metricCount + 'GroupByContainer');
	var $groupByContainer = $('#metric-' + metricCount + 'GroupByContainer');

	// Listen to aggregator button
	var groupByButton = $metricContainer.find("#addGroupByButton");

	// Listen to groupBy button
	groupByButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-plus'
		}
	}).click(function () {
			addGroupBy($groupByContainer)
		});

	// Add scale checkbox
	if (metricCount < 1) {
		$metricContainer.find(".checkbox").hide();
	}

	// Tell tabs object to update changes
	var $tabs = $("#tabs");
	$tabs.tabs("refresh");

	// Activate newly added tab
	var lastTab = $(".ui-tabs-nav").children().size() - 1;
	$tabs.tabs({active: lastTab});
}

function addGroupBy(container) {
	// Clone groupBy template
	var $groupByContainer = $("#groupByTemplate").clone();
	$groupByContainer.removeAttr("id").appendTo(container);

	// Add remove button
	var removeButton = $groupByContainer.find(".removeGroupBy");
	removeButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-close'
		}
	}).click(function () {
			$groupByContainer.remove();
		});

	var name = $groupByContainer.find(".groupByName");
	name.change(function () {
		var groupByContainer = $groupByContainer.find(".groupByContent");

		// Remove old group by
		groupByContainer.empty();

		var newName = $groupByContainer.find(".groupByName").val();
		if (newName == "tags") {
			handleTagGroupBy(groupByContainer);
		}
		else if (newName == "time") {
			$groupBy = $("#groupByTimeTemplate").clone();
			$groupBy.removeAttr("id").appendTo(groupByContainer);
			$groupBy.show();

		}
		else if (newName == "value") {
			$groupBy = $("#groupByValueTemplate").clone();
			$groupBy.removeAttr("id").appendTo(groupByContainer);
			$groupBy.show();
		}
	});

	// Set default to Tags group by and cause event to happen
	name.val("tags");
	name.change();

	$groupByContainer.show();
}

function handleTagGroupBy(groupByContainer) {
	// Clone groupBy tag template
	$groupBy = $("#groupByTagsTemplate").clone();
	$groupBy.removeAttr("id").appendTo(groupByContainer);

	// Add search button
	var searchButton = $groupBy.find(".tagSearch");
	searchButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-search'
		}
	}).click(function () {
			var $groupByTagDialog = $("#groupByTagDialog");
			$groupByTagDialog.dialog("open");
			$groupByTagDialog.dialog({position: {my: "left bottom", at: "right bottom", of: searchButton}});
			$groupByTagDialog.keypress(function (e) {
				var code = (e.keyCode ? e.keyCode : e.which);
				if (code == 13) // ENTER key
					addTagNameToGroupBy();
			});

			$("#autocompleteTagName").focus();

			$("#addTagNameButton").click(function () {
				addTagNameToGroupBy();
			});
		});

	$groupBy.show();
}

function addTagNameToGroupBy() {
	var $autocompleteTagName = $("#autocompleteTagName");
	var value = $groupBy.find(".groupByTagsValue");
	value.val(value.val() + " " + $autocompleteTagName.val());
	$autocompleteTagName.val(""); // clear value

	$("#addTagNameButton").unbind("click");

	$("#groupByTagDialog").dialog("close");
}

function addAggregator(container) {
	var aggregators = container.find(".aggregator");

	if (aggregators.length > 0) {
		// Add arrow
		$('<span class="ui-icon ui-icon-arrowthick-1-s aggregatorArrow" style="margin-left: 45px;"></span>').appendTo(container);
	}

	var $aggregatorContainer = $("#aggregatorTemplate").clone();
	$aggregatorContainer.removeAttr("id").appendTo(container);
	$aggregatorContainer.show();

	// Add remove button
	var removeButton = $aggregatorContainer.find(".removeAggregator");
	removeButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-close'
		}
	}).click(function () {
			if (container.find(".aggregator").length > 0) {
				if (!$aggregatorContainer.prev().hasClass('aggregatorArrow')) {
					// remove arrow after top aggregator
					$aggregatorContainer.next().remove();
				}
				else {
					// remove arrow pointing to this aggregator
					$aggregatorContainer.prev().remove();
				}
			}
			$aggregatorContainer.remove();
		});


	// Add listener for aggregator change
	$aggregatorContainer.find(".aggregatorName").change(function () {
		var name = $aggregatorContainer.find(".aggregatorName").val();

		if (name == "rate") {
			$aggregatorContainer.find(".aggregatorSampling").hide();
			$aggregatorContainer.find(".aggregatorPercentile").hide();
			$aggregatorContainer.find(".divisor").hide();
			$aggregatorContainer.find(".aggregatorSamplingUnit").show();

			// clear values
			$aggregatorContainer.find(".aggregatorSamplingValue").val("");
		}
		else if (name == "histogram") {
			$aggregatorContainer.find(".divisor").hide();
			$aggregatorContainer.find(".aggregatorPercentile").show().css('display', 'table-cell');
			$aggregatorContainer.find(".aggregatorSampling").show();
		}
		else if (name == "div") {
			$aggregatorContainer.find(".aggregatorSampling").hide();
			$aggregatorContainer.find(".aggregatorPercentile").hide();
			$aggregatorContainer.find(".aggregatorSamplingUnit").hide();

			$aggregatorContainer.find(".divisor").show();
		}
		else {
			$aggregatorContainer.find(".aggregatorSampling").show();
			$aggregatorContainer.find(".aggregatorSamplingUnit").show();
			$aggregatorContainer.find(".aggregatorPercentile").hide();
			$aggregatorContainer.find(".divisor").hide();
		}
	});
}

function addAutocomplete(metricContainer) {
	metricContainer.find(".metricName")
		.autocomplete({
			source: metricNames
		});
}

function addTag(tagContainer) {

	var newDiv = $("<div></div>");
	tagContainer.append(newDiv);
	$("#tagContainer").clone().removeAttr("id").appendTo(newDiv);

	// add auto complete
	var $tagNameElement = newDiv.find("[name='tagName']");
	$tagNameElement.autocomplete({
		source: function (request, response) {
			var metricCount = tagContainer.attr("metricCount");
			var metricName = $('#metricContainer' + metricCount).find(".metricName").val();
			if (metricName)
				getTagsForMetric(metricName, request, response);
		}
	});

	// add auto complete
	newDiv.find("[name='tagValue']").autocomplete({
		source: function (request, response) {
			var metricCount = tagContainer.attr("metricCount");
			var metricName = $('#metricContainer' + metricCount).find(".metricName").val();
			var tagName = $tagNameElement.val();
			if (metricName && tagName)
				getValuesForTag(metricName, tagName, request, response);
		}
	});

	// Add remove button
	var removeButton = newDiv.find(".removeTag");
	removeButton.button({
		text: false,
		icons: {
			primary: 'ui-icon-close'
		}
	}).click(function () {
			newDiv.remove();
		});
}

function getTagsForMetric(metricName, request, response) {
	var query = new kairosdb.MetricQuery();
	query.addMetric(new kairosdb.Metric(metricName));
	query.setStartAbsolute(0);

	$.ajax({
		type: "POST",
		url: "api/v1/datapoints/query/tags",
		headers: { 'Content-Type': ['application/json']},
		data: JSON.stringify(query),
		dataType: 'json',
		success: function (data) {
			var tagNames = [];
			$.each(data.queries[0].results[0].tags, function (tag) {
				tagNames.push(tag);
			});

			var matcher = new RegExp($.ui.autocomplete.escapeRegex(request.term), "i");
			response($.grep(tagNames, function (tag) {
				return matcher.test(tag);
			}));
		},
		error: function (jqXHR, textStatus, errorThrown) {
			console.log(errorThrown);
		}
	});
}

function getValuesForTag(metricName, tagName, request, response) {
	var query = new kairosdb.MetricQuery();
	query.addMetric(new kairosdb.Metric(metricName));
	query.setStartAbsolute(0);

	$.ajax({
		type: "POST",
		url: "api/v1/datapoints/query/tags",
		headers: { 'Content-Type': ['application/json']},
		data: JSON.stringify(query),
		dataType: 'json',
		success: function (data) {
			var values = [];
			$.each(data.queries[0].results[0].tags, function (tag, val) {
				if (tag == tagName){
					values = val;
					return false; //break;
				}
			});

			var matcher = new RegExp($.ui.autocomplete.escapeRegex(request.term), "i");
			response($.grep(values, function (item) {
				return matcher.test(item);
			}));
		},
		error: function (jqXHR, textStatus, errorThrown) {
			console.log(errorThrown);
		}
	});
}

function showChartForQuery(subTitle, query, metricData) {
	kairosdb.dataPointsQuery(query, function (queries) {
		showChart(subTitle, queries, metricData);
		$("#deleteButton").button("enable");
	});
}

function showChart(subTitle, queries, metricData) {
	if (queries.length == 0) {
		return;
	}

	yaxis = [];
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
			axis.position = 'right'; // Flot
			axis.opposite = 'true'; // Highcharts
			yaxis.push(axis);
			axisCount++;
		}

		resultSet.results.forEach(function (queryResult) {

			var groupByMessage = "";
			var groupBy = queryResult.group_by;
			if (groupBy) {
				$.each(groupBy, function (index, group) {
					groupByMessage += '<br>(' + group.name + ': ';

					var first = true;
					$.each(group.group, function (key, value) {
						if (!first)
							groupByMessage += ", ";
						groupByMessage += key + '=' + value;
						first = false;
					});

					groupByMessage += ')';

				});
			}

			var result = {};
			result.name = queryResult.name + groupByMessage;
			result.label = queryResult.name + groupByMessage;
			result.data = queryResult.values;
			result.yaxis = axisCount; // Flot
			result.yAxis = axisCount - 1; // Highcharts

			dataPointCount += queryResult.values.length;
			data.push(result);
		});
		metricCount++;
	});

	$("#numDataPoints").html(dataPointCount);

	var $status = $('#status');
	if (dataPointCount > 20000) {
		var response = confirm("You are attempting to plot more than 20,000 data points.\nThis may take a long time." +
			"\nYou may want to down sample your data.\n\nDo you want to continue?");
		if (response != true) {
			$status.html("Plotting canceled");
			return;
		}
	}

	if (isHighChartsLoaded())
		showHighChartsChart(subTitle, yaxis, data);
	else
		showFlotChart(subTitle, yaxis, data);
	$status.html("");
}

function isHighChartsLoaded() {
	try {
		Highcharts.charts;
		return true;
	}
	catch (err) {
		return false;
	}
}

function deleteDataPoints() {
	if (confirm("Are you sure you want to delete all data points returned from the last query?")) {
		var query = $("#query-hidden-text").val();
		kairosdb.deleteDataPoints(query, function () {
			if (confirm("Data was deleted. It may take up 30 seconds are more to update. Do you want to refresh the graph?")) {
				updateChart();
			}
		});
	}
}



