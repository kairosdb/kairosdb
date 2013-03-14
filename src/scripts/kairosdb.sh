#!/bin/bash

# Find the location of the bin directory and change to the root of kairosdb
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


# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=java
fi

pid=$TSDB_PID_DIR/kairosdb.pid

# Load up the classpath
for jar in $TSDB_LIB_DIR/*.jar; do
	CLASSPATH="$CLASSPATH:$jar"
done


if [ "$1" = "run" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH org.kairosdb.core.Main -c run -p conf/kairosdb.properties
elif [ "$1" = "start" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH org.kairosdb.core.Main \
		-c run -p conf/kairosdb.properties >> "$TSDB_LOG_DIR/tsdb.log" 2>&1 &
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
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH org.kairosdb.core.Main -c export -p conf/kairosdb.properties $*
elif [ "$1" = "import" ] ; then
	shift
	exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH org.kairosdb.core.Main -c import -p conf/kairosdb.properties $*
else
	echo "Unrecognized command."
	exit 1
fi



