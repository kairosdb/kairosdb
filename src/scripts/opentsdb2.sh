#!/bin/bash

# Find the location of the bin directory and change to the root of opentsdb2
TSDB_BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$TSDB_BIN_DIR/.."

TSDB_LIB_DIR="lib"
TSDB_LOG_DIR="log"
JAVA_OPTS=

if [ ! -d "$TSDB_LOG_DIR" ]; then
	mkdir "$TSDB_LOG_DIR"
fi

if [ "$TSDB_PID_DIR" = "" ]; then
	TSDB_PID_DIR=/tmp
fi


if [ "$JAVA_HOME" = "" ]; then
	echo "Error: JAVA_HOME is not set."
	exit 1
fi

JAVA=$JAVA_HOME/bin/java

pid=$TSDB_PID_DIR/opentsdb.pid

# Load up the classpath
for jar in $TSDB_LIB_DIR/*.jar; do
	CLASSPATH="$CLASSPATH:$jar"
done


if [ "$1" = "run" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH net.opentsdb.core.Main -c run -p conf/opentsdb.properties
elif [ "$1" = "start" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH net.opentsdb.core.Main \
		-c run -p conf/opentsdb.properties >> "$TSDB_LOG_DIR/tsdb.log" 2>&1 &
	echo $! > "$pid"
elif [ "$1" = "stop" ] ; then
	shift
	kill `cat $pid` > /dev/null 2>&1
	while kill -0 `cat $pid` > /dev/null 2>&1; do
		echo -n "."
		sleep 1;
	done
	rm $pid
elif [ "$1" = "export" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH net.opentsdb.core.Main -c export -p conf/opentsdb.properties $*
else
	echo "Unrecognized command."
	exit 1
fi



