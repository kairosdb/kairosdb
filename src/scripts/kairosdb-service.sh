#!/bin/bash
#
# chkconfig: 35 90 12
# description: KairosDB server
#
# Get function from functions library
# Start the service KairosDB

#Set JAVA_HOME if your java is not in the path already
#export JAVA_HOME=/etc/alternatives/jre

KAIROS_SCRIPT_PATH="/opt/kairosdb/bin/kairosdb.sh"
export KAIROS_PID_FILE="/var/run/kairosdb.pid"

start() {
        printf "%-50s" "Starting KairosDB server: "
        $KAIROS_SCRIPT_PATH start
        echo
}

# Restart the service KairosDB
stop() {
        printf "%-50s" "Stopping KairosDB server: "
        $KAIROS_SCRIPT_PATH stop
        echo
}

### main logic ###
case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  status)
        pid=`ps ax | grep -i 'org.kairosdb.core.Main' | grep -v grep | awk '{print $1}'`
        if [ -n "$pid" ]
          then
          echo "KairosDB is running at PID: $pid"
        else
          echo "KairosDB is not Running"
        fi
        ;;
  restart|reload|condrestart)
        stop
        start
        ;;
  *)
        echo $"Usage: $0 {start|stop|restart|reload|status}"
        exit 1
esac
exit 0
