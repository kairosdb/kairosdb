function exportCsvQueryData() {
	clear();

	var query = buildKairosDBQuery();

	if (query) {
		var offset = new Date().getTimezoneOffset() * -60000;
		var oBuilder = "Epoch Time,Local Time,Value,Groups...\n";
		kairosdb.dataPointsQuery(query, function (queries) {
			queries.forEach(function (resultSet) {
				resultSet.results.forEach(function (queryResult) {
					var groupByMessage = "";
					var groupBy = queryResult.group_by;
					if (groupBy) {
						$.each(groupBy, function (index, group) {
							if (group.group) {
								if (groupByMessage != "")
									groupByMessage += "," + group.name;
								else
									groupByMessage += "group_by:" + group.name;
								var first = true;
								groupByMessage += "(";
								$.each(group.group, function (key, value) {
									if (!first)
										groupByMessage += ", ";
									groupByMessage += key + '=' + value;
									first = false;
								});
								groupByMessage += ")";
							}
						});
					}

					queryResult.values.forEach(function (value) {
						oBuilder += value[0].toString() + ",=((" + value[0].toString() + "+" + offset.toString() + ")/86400000)+25569" + "," + value[1].toString() + "," + groupByMessage + "\n";
					});
				});

				var blob = new Blob([oBuilder], {type: "text/csv;charset=utf-8"});
				saveAs(blob, "query_json.csv");

				$('#query-hidden-text').val(JSON.stringify(query, null, 2));
				displayQuery();
				$('#status').html("");
			});

		});
	}
	$("#saveDropdown").dropdown('hide');
}

function exportJsonQueryData() {
	clear();

	var query = buildKairosDBQuery();

	if (query) {
		debugger;
		kairosdb.dataPointsQuery(query, function (resultSet) {
			var blob = new Blob([JSON.stringify(resultSet)], {type: "text/json;charset=utf-8"});
			saveAs(blob, "query_json.txt");

			$('#query-hidden-text').val(JSON.stringify(query, null, 2));
			displayQuery();
			$('#status').html("");
		});
	}
	$("#saveDropdown").dropdown('hide');
}