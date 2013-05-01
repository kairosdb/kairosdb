#!/bin/sh

# Script that checks for errors in the log file. Designed to be used by Nagios.

logFile=/opt/kairosdb/log/kairosdb.log
positionFile=/tmp/.checkLogPosition

lastPosition=0;
if [ -e $positionFile ]; then
	# Read last byte read
	lastPosition=`cat $positionFile`
fi

fileLength=`stat -c %s $logFile`
if [ $lastPosition -gt $fileLength ]; then
	# Log file rolled
	lastPosition=0
fi

difference=`expr $fileLength - $lastPosition`

# store new position to file
echo $fileLength > $positionFile

# Grep from last position to end of file for errors
errorCount=`cat $logFile | tail -c +$lastPosition | head -c +$difference | grep -i error | wc -l`

if [ "$errorCount" -gt "0" ]; then
	echo "ERROR"
	exit 2
fi

echo "SUCCESS"

