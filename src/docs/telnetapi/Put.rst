===
Put
===

You can submit data either with the telnet protocol on port 4242. The port can be changed in the kairosdb.properties file.

The format of the data is
::

	put <metric name> <time stamp> <value> <tag> <tag>... \n


**Metric name** must be one word and is limited to to utf8 characters.

**Time stamp** can either be in milliseconds or in seconds since Jan 1, 1970 (unix epoch).  If the value is less than 3,000,000,000 it is considered seconds.  If you want to send milliseconds you may want to consider using putm.

**Value** can either be a long or double value.

**Tag** is in the form of key=value.

Be aware that the data sent must be followed by a line feed character.

Here is a simple shell script that inserts data using netcat.
::

	#!/bin/bash

	# Current time in milliseconds
	now=$(($(date +%s%N)/1000000))
	metric=load_value_test
	value=42
	host=10.92.4.4

	echo "put $metric $now $value host=A" | nc -w 30 $host 4242
