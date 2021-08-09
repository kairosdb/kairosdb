=======================
Multi Cluster Cassandra
=======================

Multi cluster was added in 1.3.0.  Multi cluster lets you configure a single write
cluster and multiple read clusters for metric data.  Here is a list of use cases this
feature was designed to help with:

* Expiring old data.
* Upgrading Cassandra
* Managing size of Cassandra cluster.


^^^^^^^^^^^
What is it?
^^^^^^^^^^^

Under kairosdb.datastore.cassandra hocon configuration key you can have two sub
keys, write_cluster and read_clusters.  The write_cluster configuration is a single
configuration object where as read_clusters is an array of cluster configurations.

Each cluster configuration contains a list of seed nodes along with a keyspace name so
multiple configurations could point to different cassandra clusters or the same one but
different keyspaces.

The write_cluster is as it says, it is the cluster where data is written to.  The
read_clusters are for reading but are not read only.  Data can be deleted from as
well as queried from read_clusters.

You can also define a meta_cluster that stores the service tables (meta data), if this is left
off then the write_cluster is used.

^^^^^^^^^^^^^
How to use it
^^^^^^^^^^^^^

The best way to describe it is by example.  Lets say you have been storing data
for a year and your cluster is getting rather large.  You can roll over to a new
write_cluster and configure the current cluster as a read cluster.  Incoming data
will be written to the new cluster and queries will be sent to both clusters.
Read clusters can also be configured with a start_time and end_time to limit what
queries are sent to the read cluster otherwise all queries are sent to all clusters
(cassandra is pretty fast at identifying if it doesn't have the data you are looking for).

When doing this cut over to the new write cluster it is a great time to install a
new version of Cassandra if one is available.  Maybe you've decide to go with Scylla
instead, so Scylla will take all the new data but your legacy Cassandra data is still
available.

After changing the write cluster to a read cluster you can downsize the cluster depending
on your query traffic.  Typically read traffic is much lighter than write traffic.

This configuration can also be used to expire old data quickly and efficiently.  Say you
want to keep data for one year.  Create a new write cluster every quarter and you keep 5
clusters.  When you create a new cluster you can drop the old one.

Have a look at the configuration_ page for more details on the individual settings.

.. _configuration: Configuration.html