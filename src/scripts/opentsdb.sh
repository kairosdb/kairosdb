#!/bin/bash

TSDB_LOG_DIR="../log"
JAVA_OPTS=

if [ "$JAVA_HOME" = "" ]; then
	echo "Error: JAVA_HOME is not set."
	exit 1
fi

JAVA=$JAVA_HOME/bin/java

if [ ! -d "$TSDB_LOG_DIR" ]; then
	mkdir "$TSDB_LOG_DIR"
fi

if [ "$TSDB_PID_DIR" = "" ]; then
	TSDB_PID_DIR=/tmp
fi

pid=$TSDB_PID_DIR/opentsdb.pid

for jar in ../lib/*.jar; do
	CLASSPATH="$CLASSPATH:$jar"
done


if [ "$1" = "run" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH net.opentsdb.core.Main -p ../conf/opentsdb.properties
elif [ "$1" = "start" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH net.opentsdb.core.Main \
		-p ../conf/opentsdb.properties >> "$TSDB_LOG_DIR/tsdb.log" 2>&1 &
	echo $! > "$pid"
elif [ "$1" = "stop" ] ; then
	shift
	kill `cat $pid` > /dev/null 2>&1
	while kill -0 `cat $pid` > /dev/null 2>&1; do
		echo -n "."
		sleep 1;
	done
	rm $pid
else
	echo "Unrecognized command."
	exit 1
fi



