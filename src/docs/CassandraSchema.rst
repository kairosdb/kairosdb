================
Cassandra Schema
================

The cassandra schema consists of 7 column families, 8 if you have the legacy row_key_index.

* data_points - where the data is kept.
* row_key_index - (legacy) index to lookup what rows to get during a query.
* row_key_time_index - Index for what time frame a metrics is stored in.
* row_keys - Index of row keys for data_points tables.
* tag_indexed_row_keys - Used to index high cardinality tags for faster lookup.
* string_index - Used to answer the query of what metrics are in the system.
* service_index - Used for storing metadata.
* spec - Place to store internal settings like row width.

The schema for each of the column families are defined in `ClusterConnection.java`

---------------
Column Families
---------------

^^^^^^^^^^^^^^^^^^^^^^^^^
Data Points Column Family
^^^^^^^^^^^^^^^^^^^^^^^^^

The row key is made up of 3 parts all concatenated together.

1. Metric name (UTF-8) null terminated.
2. Row time stamp (64 bits).  The time stamp for the row to begin at.
3. Datastore type (UTF-8).  Length preceded (1 byte) utf-8 string with null at the end.  Yes kinda redundant but it works nice.
4. Concatenated string of tags (tag1=val1:tag2=val2...) (UTF-8)

The column name is different depending on of it from the legacy row_key_index or not.

**Legacy Column Name**:
The column name is 32 bits of data.  The first 31 bits is the unsigned time offset
from the row key (in milliseconds).  The last bit is unused.

**Current Column Name**: 32 bit value (no shifting and unsigned) that represents the number of time units
from the row key timestamp.  The time unit is either seconds or milliseconds
depending on how the cluster was configured when first created.

The value of the column varies depending on the type of value.  The exact format
is defined by the ``writeValueToBuffer`` method on the DataPoint.

The length of the row defaults to exactly three weeks of data or 1,814,400,000 columns.
The width of the row can be changed in the cluster configuration when the cluster
is created.  It cannot be changed after the schema has been created.

^^^^^^^^^^^^^^^^^^^^^^^^^^^
Row Key Index Column Family
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Legacy** This row is primarily used when querying the data.  The row key is the name of
the metric.  The names of the columns are the row keys from the data_points column
family.  The columns have no values.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Row Key Time Index Column Family
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The purpose of this table is to index the time frame a metric shows up for.  It also
provides a future mapping if we ever want to get away from the data points table.
This is the first table hit when doing a query.

^^^^^^^^^^^^^^^^^^^^^^
Row Keys Column Family
^^^^^^^^^^^^^^^^^^^^^^

This is the second table hit when doing a query.  The values in this table have
all the combinations of tags that show up for the specified metric.  Results from
this table are filtered if any tags are specified in the query.  The results are then
used to lookup the actual data in the data points table.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Tag Indexed Row Keys Column Family
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a metric is configured with an additional index this table is used for queries
under certain circumstances.  When you configure a metric to be indexed on a tag,
say the host tag, data will be written to this table as well as the row keys table.
If you do a lookup on the metric and specify a host tag in the query the query will use
this table to speed up the query results.  if no host tag is specified then the query
will use the row keys table like usual.

^^^^^^^^^^^^^^^^^^^^^^^^^^
String Index Column Family
^^^^^^^^^^^^^^^^^^^^^^^^^^

Just an index to lookup what metric names, tag names and tag values are in the
system.  There are three rows one for each of the above mentioned.

^^^^^^^^^^^^^^^^^^^^^^^^^^^
Service Index Column Family
^^^^^^^^^^^^^^^^^^^^^^^^^^^

All of the information stored using the metadata api is stored in this table.

^^^^^^^^^^^^^^^^^^
Spec Column Family
^^^^^^^^^^^^^^^^^^

This is an internal table currently only used for storing row width and time granularity.
The values in this table are not to be changed by external means.

----------------------
How the Schema is Used
----------------------

When a query comes in a column slice of the row key index is done for the
particular metric, this returns the rows that will contain the data.  The row
keys are then filtered based on if any tags were specified.  A multi get hector
call is made to fetch the data from the various rows.  If any row has more data
then the remainder is fetched individually using a larger buffer.

--------------------------------
Row width and second granularity
--------------------------------

By default the width of a row of data in the data points table is about 3 weeks and
the time granularity of the data is in milliseconds.  In the 1.3.0 release configuration
options were introduced to allow the user to change those values durring the creation of
the schema.  (See `kairosdb.conf` file)  The values for row_time_unit and row_width are
only read when creating the schema for the write cluster.  After the initial creation
the values are stored in the spec table and must not be changed or data loss will occur.
