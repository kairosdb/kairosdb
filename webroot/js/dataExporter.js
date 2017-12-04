function exportCsvQueryData(result) {
	if (result) {
		var offset = new Date().getTimezoneOffset() * -60000;
		result.forEach(function (resultSet) {
			var oBuilder = "Epoch Time,Local Time,Value,Groups...\n";
			resultSet.results.forEach(function (queryResult) {
				var groupByMessage = "";
				var groupBy = queryResult.group_by;
				if (groupBy) {
					$.each(groupBy, function (index, group) {
						if(group.group) {
							groupByMessage += "group_by:" + group.name + ',';
							var first = true;
							$.each(group.group, function (key, value) {
								if (!first) groupByMessage += ", ";
								groupByMessage += key + '=' + value;
								first = false;
							});
						}
					});
				}

				queryResult.values.forEach(function (value) {
					oBuilder += value[0].toString() + ",=((" + value[0].toString() + "+" + offset.toString() + ")/86400000)+25569" + "," + value[1].toString() + "," + groupByMessage + "\n";
				});
			});

			if (!resultSet.results.length) return;

			var blob = new Blob([oBuilder], {type: "text/csv;charset=utf-8"});
			saveAs(blob, resultSet.results[0].name + ".csv");
		});
	}
}

function exportJsonQueryData(result) {
	if (result) {
		var blob = new Blob([JSON.stringify(result)], {type: "text/json;charset=utf-8"});
		saveAs(blob, "query.json");
	}
}
