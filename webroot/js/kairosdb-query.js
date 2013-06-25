if (kairosdb === undefined)
{
	var kairosdb = {};
}

kairosdb.dataPointsQuery = function (metricQuery, callback) {
	var startTime = new Date();

	var $status = $('#status');
	var $queryTime = $("#queryTime");

	$status.html("<i>Query in progress...</i>");

	$.ajax({
		type: "POST",
		url: "api/v1/datapoints/query",
		headers: { 'Content-Type': ['application/json']},
		data: JSON.stringify(metricQuery),
		dataType: 'json',
		success: function (data, textStatus, jqXHR) {
			$status.html("<i>Plotting in progress...</i>");
			$queryTime.html(new Date().getTime() - startTime.getTime() + " ms");
			setTimeout(function(){
				callback(data.queries);
			}, 0);
		},
		error: function (jqXHR, textStatus, errorThrown) {

			var $errorContainer = $("#errorContainer");
			$errorContainer.show();
			$errorContainer.html("");
			$errorContainer.append("Status Code: " +  jqXHR.status + "</br>");
			$errorContainer.append("Status: " +  jqXHR.statusText + "<br>");
			$errorContainer.append("Return Value: " +  jqXHR.responseText);

			$status.html("");
			$queryTime.html(new Date().getTime() - startTime.getTime() + " ms");
		}
	});
};
