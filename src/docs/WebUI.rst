======
Web UI
======

KairosDB has the ability to serve up content found in the webroot folder.  With a default install you can point your browser to `http://localhost:8080` and it serve up a query page whereby you can query data within the data store.

The UI is designed primarily for development purposes.  After you click on Load Graph the query you performed will show up in the Query JSON box.

By default the UI uses [http://www.flotcharts.org/ Flot] to create charts because it's an open source project. If you would prefer to use [http://www.highcharts.com/ Highcharts] then do the following:

#. Download highcharts and save in the webroot/js directory.
#. Uncomment the highcharts.js include line in webroot/index.html.
#. Uncomment the highcharts.js include line in webroot/view.html.