=================
Graphite Protocol
=================

Kairos can accept the Graphite plaintext and pickle protocol by means of the
`Kairos-carbon plugin <https://github.com/kairosdb/kairos-carbon>`_.  The Graphite
protocols are explained `here <https://graphite.readthedocs.org/en/latest/feeding-carbon.html>`_.
This feature is for ingesting data only.  This lets you push data to Kairos
from applications that normally push to Graphite.
You can also configure Carbon relay servers to send data to Kairos for long term storage.

--------------------------
Enabling Graphite Protocol
--------------------------

Turning on the Graphite plaintext and pickle protocol is done by enabling the CarbonServerModule in the kairosdb.properties file.
::

	kairosdb.service.carbon=org.kairosdb.core.carbon.CarbonServerModule

-----------------------------
Configuring Graphite Protocol
-----------------------------

The following properties control the Graphite protocol handlers

+---------------------------------+--------------------------------------------------------------------------------+
| kairosdb.carbon.tagparser       | Class name for the tag parser (default org.kairosdb.core.carbon.HostTagParser) |
+---------------------------------+--------------------------------------------------------------------------------+
| kairosdb.carbon.text.port       | Plaintext port (default 2003)                                                  |
+---------------------------------+--------------------------------------------------------------------------------+
| kairosdb.carbon.pickle.port     | Pickle port (default 2004)                                                     |
+---------------------------------+--------------------------------------------------------------------------------+
| kairosdb.carbon.pickle.max_size | Size of buffer to allocate for incoming pickles (default 2048)                 |
+---------------------------------+--------------------------------------------------------------------------------+

The following settings are used with the HostTagParser class.  As Kairos uses tags and requires at least one tag a tag must be derived from the metric name.  The HostTagParser uses simple regular expressions to identify a host tag.  The default settings below show extracting the second value in a dot notation metric name as the host.  For example service.host_name.some_metric.

+---------------------------------------------------+---------------------------------------------------------------------------------------+
| kairosdb.carbon.hosttagparser.host_pattern        | Pattern for finding the host name (default '`[^.]*\.([^.]*)\..*`')                    |
+---------------------------------------------------+---------------------------------------------------------------------------------------+
| kairosdb.carbon.hosttagparser.host_replacement    | Used in combination with the above to come up with a host name (default '`$1`')       |
+---------------------------------------------------+---------------------------------------------------------------------------------------+
| kairosdb.carbon.hosttagparser.metric_pattern      | Pattern for finding the metric name (default '`([^.]*)\.[^.]*\.(.*)`')                |
+---------------------------------------------------+---------------------------------------------------------------------------------------+
| kairosdb.carbon.hosttagparser.metric_replacement  | Used in combination with the above to come up with a metric name (default '`$1.$2`')  |
+---------------------------------------------------+---------------------------------------------------------------------------------------+

-----------------
Custom Tag Parser
-----------------
To implement your own tag parser just implement org.kairosdb.core.carbon.TagParser interface and specify your new class with the `kairosdb.carbon.tagparser` property in kairosdb.properties.