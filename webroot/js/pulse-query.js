if (pulse === undefined)
{
	var pulse = {};
}

pulse.dataPointsQuery = function (metricQuery, callback) {
	var startTime = new Date();

	var $queryTime = $("#queryTime");
	$queryTime.html("");
	$queryTime.html("<i>in progress...</i>");

	$.ajax({
		type: "POST",
		url: "/api/v1/datapoints/query",
		headers: { 'Content-Type': ['application/json']},
		data: JSON.stringify(metricQuery),
		dataType: 'json',
		success: function (data, textStatus, jqXHR) {
			$queryTime.html("");
			$queryTime.append(new Date().getTime() - startTime.getTime() + " ms");
			callback(data.queries);
		},
		error: function (jqXHR, textStatus, errorThrown) {

			var $errorContainer = $("#errorContainer");
			$errorContainer.show();
			$errorContainer.html("");
			$errorContainer.append("Status Code: " +  jqXHR.status + "</br>");
			$errorContainer.append("Status: " +  jqXHR.statusText + "<br>");
			$errorContainer.append("Return Value: " +  jqXHR.responseText);

			$queryTime.html("");
			$queryTime.append(new Date().getTime() - startTime.getTime() + " ms");
		}
	});
};
