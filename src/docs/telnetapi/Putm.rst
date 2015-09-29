====
Putm
====

This is identical to the put command except it always expects millisecond timestamps.

You can submit data either with the telnet protocol on port 4242. The port can be changed in the kairosdb.properties file.

The format of the data is
::

	putm <metric name> <time stamp> <value> <tag> <tag>... \n


**Metric name** must be one word and is limited to utf8 characters.

**Time stamp** milliseconds since Jan 1, 1970 (unix epoch)

**Value** can either be a long or double value.

**Tag** is in the form of key=value.

Be aware that the data sent must be followed by a line feed character.

Here is a simple shell script that inserts data using netcat.

.. code-block:: bash

	#!/bin/bash

	# Current time in milliseconds
	now=$(($(date +%s%N)/1000000))
	metric=load_value_test
	value=42
	host=10.92.4.4

	echo "putm $metric $now $value host=A" | nc -w 30 $host 4242
