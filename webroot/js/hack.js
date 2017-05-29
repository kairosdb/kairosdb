function queryAndGraph()
{
    var eventId = document.getElementById("eventId").value;
    var query = {
        metrics: [
            {
                tags: {},
                name: "hack." + eventId
            }
        ],
        cache_time: 0,
        start_relative: {
            value: "1",
            unit: "hours"
        }
    };

    kairosdb.dataPointsQuery(query, function (queries) {
        var values = queries[0].results[0].values;
        drawGraph(values);
    });
}

function drawGraph(values)
{
    // Create the input graph
    var g = new dagreD3.graphlib.Graph()
            .setGraph({
                rankdir: "LR" // Shift graph from left to right rather than top to bottom
            })
            .setDefaultEdgeLabel(function() { return {}; });

    // Add event type as a node
    var eventType = JSON.parse(values[0][1])["eventType"];
    g.setNode(0, {label: eventType, class: "type-TK"});

    var edgeMap = {};
    var counter = 1;
    values.forEach(function(value){
        var eventTrace = JSON.parse(value[1]);
        eventTrace["edgeId"] = counter;
        if (eventTrace["isChild"])
        {
            g.setNode(counter, {label: eventTrace.eventType, class: "type-TK" })
        }
        else {
            g.setNode(counter, {label: eventTrace.service});
        }
        edgeMap[eventTrace["ip"]] = eventTrace;
        counter++;
    });

    g.setEdge(0, 1);

    values.forEach(function(value)
    {
        var eventTrace = JSON.parse(value[1]);
        var source = edgeMap[eventTrace["sourceIP"]];
        if (source != undefined) {
            g.setEdge(edgeMap[source["ip"]]["edgeId"], edgeMap[eventTrace["ip"]]["edgeId"]);
        }
    });

    //
    // // Here we"re setting nodeclass, which is used by our custom drawNodes function
    // // below.
    // g.setNode(10,  { label: "TOP",       class: "type-TOP" });
    // g.setNode(11,  { label: "S",         class: "type-S" });
    // g.setNode(12,  { label: "NP",        class: "type-NP" });
    // g.setNode(13,  { label: "DT",        class: "type-DT" });
    // g.setNode(14,  { label: "This",      class: "type-TK" });
    // g.setNode(15,  { label: "VP",        class: "type-VP" });
    // g.setNode(16,  { label: "VBZ",       class: "type-VBZ" });
    // g.setNode(17,  { label: "is",        class: "type-TK" });
    // g.setNode(8,  { label: "NP",        class: "type-NP" });
    // g.setNode(9,  { label: "DT",        class: "type-DT" });
    // g.setNode(10, { label: "an",        class: "type-TK" });
    // g.setNode(11, { label: "NN",        class: "type-NN" });
    // g.setNode(12, { label: "example",   class: "type-TK" });
    // g.setNode(13, { label: ".",         class: "type-." });
    // g.setNode(14, { label: "sentence",  class: "type-TK" });
    //
    g.nodes().forEach(function(v) {
        var node = g.node(v);
        // Round the corners of the nodes
        node.rx = node.ry = 5;
    });

    // Create the renderer
    var render = new dagreD3.render();

    // Set up an SVG group so that we can translate the final graph.
    var svg = d3.select("svg"),
            svgGroup = svg.append("g");

    // Run the renderer. This is what draws the final graph.
    render(d3.select("svg g"), g);

    // Center the graph
    var xCenterOffset = (svg.attr("width") - g.graph().width) / 2;
    svgGroup.attr("transform", "translate(" + xCenterOffset + ", 20)");
    svg.attr("height", g.graph().height + 40);

}