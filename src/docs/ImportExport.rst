============================
Importing and Exporting Data
============================

Import and export is available on the KairosDB server from the command line.

--------------
Exporting Data
--------------

To export data from KairosDB run the following command:
::

	> bin/kairosdb.sh export -f export.txt

The format of the export is one metric per line in the form of a json object.  This can be really verbose but it lets you do interesting things with the data after exporting.  If size is an issue try this:
::

	> bin/kairosdb.sh export | gzip > export.gz

^^^^^^^^^^^^^^^
Export Switches
^^^^^^^^^^^^^^^

-f `<filename>` -- file to write output to. If not specified, the output goes to stdout.

-n `<metricName>` -- name of metric to export. If not specified, then all metrics are exported.

--------------
Importing Data
--------------

To import the data we exported in the step above you can do this:
::

	> bin/kairosdb.sh import -f export.txt

If you happened to compress the export you can pipe it back into the system like this:
::

	> gzip -dc export.gz | bin/kairosdb.sh import

^^^^^^^^^^^^^^^
Import Switches
^^^^^^^^^^^^^^^

-f `<filename>` -- file to import. If not specified the input comes from stdin.

-------------------
Performance Numbers
-------------------

Here is the performance of the import process.

The machine: Intel i5 (4 cores) 12 Gigs ram.  The machine has two drives one SSD and the other is a platter drive.  Cassandra (single instance) and KairosDB are running on the same machine.

The data:  31,341,782 metrics, the majority of which is the same metric and tags, which means it will be writing into a single row.

Results

+------------------+-------------------------------------+--------------------+
| Cassandra Memory | Location of Data                    | Metrics per second |
+------------------+-------------------------------------+--------------------+
| 1 Gig            | data and commit log on SSD          | 74,623             |
+------------------+-------------------------------------+--------------------+
| 1 Gig            | data on platter, commit log on SSD  | 93,837             |
+------------------+-------------------------------------+--------------------+
| 2 Gig            | data on platter, commit log on SSD  | 132,804            |
+------------------+-------------------------------------+--------------------+
