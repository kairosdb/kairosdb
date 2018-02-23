###############
Getting Started
###############

=======
Install
=======

KairosDB runs with Java 1.8 or later.

#. Download the tar.gz file from the Downloads section
#. Extract to where you wish to run from
#. In conf/kairosdb.properties change the kairosdb.service.datastore property to the datastore you wish to use.  It defaults to an in memory H2 database (that is slow)
#. Make sure that JAVA_HOME is set to your java install.
#. Change to the bin directory and run ``>./kairosdb.sh run``

-----------------------------------
Changing File Handle Limit on Linux
-----------------------------------

If you have a lot of clients pushing metrics, you may run out of file handles. We recommend increasing the number of file handles. Here is an example of [http://tech-torch.blogspot.com/2009/07/linux-ubuntu-tomcat-too-many-open-files.html changing file handles on CentOS].

------------------
Changing Datastore
------------------

KairosDB can be configured to use one of several backends for storing data.  By default KairosDB is configured to use an in memory H2 database to store datapoints.  To change the datastore that is used change the ``kairosdb.service.datastore`` property in the kairosdb.properties file.

-------------
Using with H2
-------------


``kairosdb.service.datastore=org.kairosdb.datastore.h2.H2Module``

By default KairosDB is configured to run using the H2 datbase.  This lets you do development work without setting up and running Cassandra.

"""""""""""""""""""""
Configuration Options
"""""""""""""""""""""

+-------------------------------------+---------------------------+
| kairosdb.datastore.h2.database_path | Location of H2 database   |
+-------------------------------------+---------------------------+

Deleting the database folder and restarting KairosDB will cause the database to be recreated with no data.


--------------------
Using with Cassandra
--------------------

``kairosdb.service.datastore=org.kairosdb.datastore.cassandra.CassandraModule``

The default configuration for Cassandra is to use wide rows.  Each row is set to contain 3 weeks of data.  The reason behind setting it to 3 weeks is if you wrote a metric every millisecond for 3 weeks it would be just over 1 billion columns.  Cassandra has a 2 billion column limit.

The row size has been a point of some confusion.  Basically it comes down to this: The more data you can fit into a single row the better the system will perform when querying the data.  This does not mean that Cassandra only holds 3 weeks worth of data, it means data is written to the same row for 3 weeks before going to a new row.  See the [CassandraSchema cassandra schema for more details].

Changing the read_repair_chance:  This value tells cassandra how often to perform a read repair.  The read repair chance will default to 1 (100% chance).  The recommended value is 0.1 (10% chance).  To change this log into the cassandra-cli client and run the following commands
::

	> use kairosdb;
	> update column family data_points with read_repair_chance = 0.1;
	> update column family row_key_index with read_repair_chance = 0.1;
	> update column family string_index with read_repair_chance = 0.1;

Note: If you are using the newer ``cqlsh`` command, you will need to use the alter syntax
::
       > use kairosdb;
       > ALTER TABLE data_points WITH read_repair_chance = 0.1;
       > ALTER TABLE row_key_index WITH read_repair_chance = 0.1;
       > ALTER TABLE string_index WITH read_repair_chance = 0.1;

"""""""""""""""""""""
Configuration Options
"""""""""""""""""""""

For a complete list of options please see the Cassandra section in kairosdb.properties.

+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.host_name            | Host name or IP address of Cassandra server                                                                                                                                                                                                        |
+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.port                 | Port number of Cassandra server                                                                                                                                                                                                                    |
+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.replication_factor   | Replication factor when writing data to Cassandra [http://www.datastax.com/docs/1.0/cluster_architecture/replication more info]                                                                                                                    |
+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.write_delay          | The amount of time a background thread waits before writing data to Cassandra.  This allows batching data to the datastore.                                                                                                                        |
+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.single_row_read_size | The number of columns read when reading a single row.  This is a balance between performance and memory usage.  This value is used when querying the row key index and when performing subsequent queries on a row after the initial multi get.    |
+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.multi_row_read_size  | The number of columns read on the initial multi get for a query.  If your data has very few tags make this number big.  If your metrics have lots of tag combinations then back this number down or you may run into out of memory issues.         |
+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

-----------------
Configuring HBase
-----------------

HBase is not longer supported.  Originally the HBase support was achieved by forking the OpenTSDB code base and making it into a Datastore plugin for KairosDB.  The functionality of KairosDB has moved beyond what HBase code can support.


---------------------------
Using as a Remote Datastore
---------------------------

``kairosdb.service.datastore=org.kairosdb.datastore.remote.RemoteModule``

Configuring Kairos as a remote datastore sets up the local Kairos instance to store and forward incoming data points to a remote Kairos server.  Data is stored in a directory in json format.  On a configurable schedule a background thread will compress the data and upload the data points to a remote KairosDB instance.

"""""""""""""""""""""
Configuration Options
"""""""""""""""""""""

+--------------------------------------+-----------------------------------------------------------------------------------------------+
| kairosdb.datastore.remote.data_dir   | Directory in which the data points are collected.  Defaults to the current directory.         |
+--------------------------------------+-----------------------------------------------------------------------------------------------+
| kairosdb.datastore.remote.remote_url | URL of the KairosDB instance to which data will be forwarded.  Ex. http://10.10.10.10:8080    |
+--------------------------------------+-----------------------------------------------------------------------------------------------+
| kairosdb.datastore.remote.schedule   | Quartz cron schedule for how often to upload collected data.                                  |
+--------------------------------------+-----------------------------------------------------------------------------------------------+

=====================
Starting and Stopping
=====================

Starting and stoping KairosDB is done by running the kairosdb.sh script from within the bin directory.

To start KairosDB and run in the foreground type
::

	> ./kairosdb.sh run

To run KairosDB as a background process type
::

	> ./kairosdb.sh start

To stop KairosDB when running as a background process type
::

	> ./kairosdb.sh stop
