#!/bin/bash
### BEGIN INIT INFO
# Provides:          kairosdb
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start daemon at boot time
# Description:       Enable service provided by daemon.
### END INIT INFO#

#Set JAVA_HOME if your java is not in the path already
#export JAVA_HOME=/etc/alternatives/jre

KAIROS_SCRIPT_PATH="/opt/kairosdb/bin/kairosdb.sh"
export KAIROS_PID_FILE="/var/run/kairosdb.pid"

if [ -f /etc/init.d/functions ]; then
    . /etc/init.d/functions
fi


# Start the service KairosDB
start() {
        if [ -f $KAIROS_PID_FILE ]; then
          echo 'Service already running'
          echo
          return 1
        fi
        printf "%-50s" "Starting KairosDB server: "
        $KAIROS_SCRIPT_PATH start
        echo
}

status() {
        if [ -f $KAIROS_PID_FILE ]; then
          echo 'Service KairosDB is up and running!'
          echo
          return 1
        fi
        echo "Service KairosDB not running." 
        echo
}

# Stop the service KairosDB
stop() {
        if [ ! -f "$KAIROS_PID_FILE" ]; then
          echo 'Service not running'
          echo
          return 1
        fi
        printf "%-50s" "Stopping KairosDB server: "
        $KAIROS_SCRIPT_PATH stop
        rm -f "$KAIROS_PID_FILE"
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
        status
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
