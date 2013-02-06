function updateChart() {
	$("#errorContainer").hide();

	var query = new pulse.MetricQuery();

	// todo cachetime

	var metricName = $('#metric').val();
	if (!metricName)
	{
		showErrorMessage("Metric Name is required.");
		return;
	}

	var aggregate = $('#aggregate').val();
	var groupBy = $('#groupBy').val();
	var metric = new pulse.Metric(metricName, aggregate, false, groupBy);

	// Add Tags
	$.each($("[name='tags']"), function (index, tagContainer) {
		var name = $(tagContainer).find("[name='tagName']").val();
		var value = $(tagContainer).find("[name='tagValue']").val();

		if (name && value)
			metric.addTag(name, value);
	});

	query.addMetric(metric);

	var startTimeAbsolute = $("#startTime").datepicker("getDate");
	var startTimeRelativeValue = $("#startRelativeValue").val();

	if (startTimeAbsolute != null) {
		query.setStartAbsolute(startTimeAbsolute.getTime());
	}
	else if (startTimeRelativeValue){
		query.setStartRelative(startTimeRelativeValue, $("#startRelativeUnit").val())
	}
	else{
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
		else {
			// Set end time to NOW
			query.setEndAbsolute(new Date().getTime());
		}
	}

	$("#query-text").val(JSON.stringify(query, null, 2));
	showChartForQuery(metricName, "", "", "", "", query);
}

function showErrorMessage(message){
	var $errorContainer = $("#errorContainer");
	$errorContainer.show();
	$errorContainer.html("");
	$errorContainer.append(message);
}

function addTag() {
	var $tagsContainer = $("#tagsContainer");
	var newDiv = $("<div></div>");
	$tagsContainer.append(newDiv);
	$("#tagContainer").clone().attr('style', "clear:both").appendTo(newDiv);

	var tagName = newDiv.find("[name='tagName']");
	tagName.val(""); // clear it value

	// add auto complete
	$(tagName).autocomplete({
		source: tagNames
	});

	var tagValue = newDiv.find("[name='tagValue']");
	tagValue.val("");

	// add auto complete
	$(tagValue).autocomplete({
		source: tagValues
	});
}

function showChartForQuery(title, subTitle, chartType, yAxisTitle, seriesTitle, query) {
	pulse.dataPointsQuery(query, function(queries)
	{
		showChart(title, subTitle, chartType, yAxisTitle, seriesTitle, queries);
	});
}

function showChart(title, subTitle, chartType, yAxisTitle, seriesTitle, queries) {
	var results = queries[0].results;

	if (results.length == 0) {
		//document.write("No Data");
		return;
	}

	var values = $.map(results[0].values, function(n){return n[1];});
	drawSingleSeriesChart(title, subTitle, chartType, yAxisTitle, seriesTitle, results[0].values);
}

function drawSingleSeriesChart(title, subTitle, chartType, yAxisTitle, seriesTitle, data) {
	chart = new Highcharts.Chart({
		chart:{
			renderTo:'container',
			type:chartType,
			marginRight:130,
			marginBottom:50,
			zoomType: 'x'
		},
		title:{
			text:title,
			x:-20 //center
		},
		subtitle:{
			text:subTitle,
			x:-20
		},
		xAxis:{
			type: 'datetime',
			labels:{
				rotation:-45
			},
			dateTimeLabelFormats: {
				second: '%H:%M:%S',
				minute: '%e %H:%M',
				hour: '%e %H',
				day: '%e. %b',
				week: '%e %b',
				month: '%b \'%y',
				year: '%Y'
			}
		},
		yAxis:{
			title:{
				text:yAxisTitle
			},
			plotLines:[
				{
					value:0,
					width:1,
					color:'#808080'
				}
			]
		},
		tooltip:{
			formatter:function () {
				return '<b>' + this.series.name + '</b><br/>' +
					this.x + ': ' + this.y;
			}
		},
		legend:{
			layout:'vertical',
			align:'right',
			verticalAlign:'top',
			x:-10,
			y:100,
			borderWidth:0
		},
		series:[
			{
				name:seriesTitle,
				data:data
			}
		]
	});
}

