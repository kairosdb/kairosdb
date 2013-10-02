
   

function exportCsvQueryData() {
    var query = buildKairosDBQuery();
    var offset = new Date().getTimezoneOffset()*-60000;
    var oBuilder = "Epoch Time,Excel Local Time,Value,Groups...\n";
    kairosdb.dataPointsQuery(query, function (queries) {
        queries.forEach(function (resultSet) {
            resultSet.results.forEach(function (queryResult) {
                var groupByMessage="";
                var groupBy = queryResult.group_by;
                if (groupBy) {
                    $.each(groupBy, function (index, group) {
                        groupByMessage += "group_by:"+group.name+',';

                        var first = true;
                        $.each(group.group, function (key, value) {
                            if (!first)
                                groupByMessage += ", ";
                            groupByMessage += key + '=' + value;
                            first = false;
                        });
                    });
                }
                
                queryResult.values.forEach(function (value) {
                    oBuilder+=value[0].toString()+",=(("+value[0].toString()+"+"+offset.toString()+")/86400000)+25569"+","+value[1].toString()+","+groupByMessage+"\n";
                });
            });
        
        
            var blob = new Blob([oBuilder], {type: "text/csv;charset=utf-8"});
            saveAs(blob, "query_json.csv"); $("#json").val($.toJSON(data));    
        });
	});
    
}

function exportJsonQueryData(){    
   var query = buildKairosDBQuery();
   kairosdb.dataPointsQuery(query, function (resultSet) {
		var blob = new Blob([JSON.stringify(resultSet)], {type: "text/json;charset=utf-8"});
        saveAs(blob, "query_json.txt"); $("#json").val($.toJSON(data));    
   });

   
}

