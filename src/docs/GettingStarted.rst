###############
Getting Started
###############

=======
Install
=======

KairosDB runs with Java 1.8 or later.

#. Download the tar.gz file from the `releases <https://github.com/kairosdb/kairosdb/releases>`_
#. Extract to where you wish to run from
#. In conf/kairosdb.conf change the ``kairosdb.service.datastore`` property to the datastore you wish to use.  It defaults to an in memory H2 database (that is slow)
#. Make sure that JAVA_HOME is set to your java install.
#. Change to the bin directory and run ``>./kairosdb.sh run``

------------------------------------
Understanding the configuration file
------------------------------------

The configuration file for Kairosdb 1.3 and later uses Hocon (https://github.com/lightbend/config/blob/main/HOCON.md)
The configuration is hierarchical and looks like json with ``kairosdb`` at the root.  Configurations with periods represent
each level of the configuration tree.

-----------------------------------
Changing File Handle Limit on Linux
-----------------------------------

If you have a lot of clients pushing metrics, you may run out of file handles. We recommend increasing the number of file handles.
Here is an example of [http://tech-torch.blogspot.com/2009/07/linux-ubuntu-tomcat-too-many-open-files.html changing file handles on CentOS].

------------------
Changing Datastore
------------------

KairosDB can be configured to use one of two backends for storing data.  By default KairosDB is configured to use an in
memory H2 database to store datapoints.  To change the datastore that is used change the ``kairosdb.service.datastore``
property in the kairosdb.conf file.
The property ``kairosdb.services.datastore`` could be located
::
    kairosdb: {
        service: {
            datastore: "<config value>"
        }
    }


-------------
Using with H2
-------------

``kairosdb.service.datastore: "org.kairosdb.datastore.h2.H2Module"``

By default KairosDB is configured to run using the H2 database.  This lets you do development work without setting up and running Cassandra.

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

``kairosdb.service.datastore: "org.kairosdb.datastore.cassandra.CassandraModule"``

The default configuration for Cassandra is to use wide rows.  Each row is set to contain 3 weeks of data.  The reason behind setting it to 3 weeks is if you wrote a metric every millisecond for 3 weeks it would be just over 1 billion columns.  Cassandra has a 2 billion column limit.

The row size has been a point of some confusion.  Basically it comes down to this: The more data you can fit into a single row the better the system will perform when querying the data.  This does not mean that Cassandra only holds 3 weeks worth of data, it means data is written to the same row for 3 weeks before going to a new row.  See the Broken:[CassandraSchema cassandra schema for more details].

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

For a complete list of options please see the Cassandra section in kairosdb.conf.

The datastore configuration is broken up into read_cluster and write_cluster.  For quick setup you just need to focus on
the write cluster configuration.  See :ref:`multi_cluster` for when to use read clusters.

+---------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.write_cluster.cql_host_list            | List of bootstrap servers used for initial connection to Cassandra                                                                                                                                                                                 |
+---------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| kairosdb.datastore.cassandra.write_cluster.replication              | Lets you set the replication strategy and replication factor when writing to Cassandra [http://www.datastax.com/docs/1.0/cluster_architecture/replication more info]                                                                               |
+---------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

---------------------------
Using as a Remote Datastore
---------------------------

The remote datastore functionality was moved into its own plugin https://github.com/kairosdb/kairos-remote

=====================
Starting and Stopping
=====================

Starting and stopping KairosDB is done by running the kairosdb.sh script from within the bin directory.

To start KairosDB and run in the foreground type
::

	> ./kairosdb.sh run

To run KairosDB as a background process type
::

	> ./kairosdb.sh start

To stop KairosDB when running as a background process type
::

	> ./kairosdb.sh stop
