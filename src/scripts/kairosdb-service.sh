#!/bin/bash
#
# kairosdb Startup scripts for KairosDB
# chkconfig: - 90 12
# description: KairosDB server
#
# Get function from functions library
# Start the service KairosDB

#Set JAVA_HOME if your java is not in the path already
#export JAVA_HOME=/etc/alternatives/jre

# Source function library.
. /etc/init.d/functions

RETVAL=0
prog="kairosdb"

export KAIROS_PID_FILE="/var/run/kairosdb.pid"

KAIROS_SCRIPT_PATH="/opt/kairosdb/bin/kairosdb.sh"

start() {
	echo -n $"Starting $prog: "
	daemon --pidfile $KAIROS_PID_FILE $KAIROS_SCRIPT_PATH start
	RETVAL=$?
	echo
	[ $RETVAL -eq 0 ] && touch /var/lock/subsys/$prog
}

stop() {
	echo -n $"Stopping $prog: "
	killproc -p $KAIROS_PID_FILE $prog
	RETVAL=$?
	echo
	[ $RETVAL -eq 0 ] && rm -f /var/lock/subsys/$prog
}

# See how we were called.
case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  status)
        status -p $KAIROS_PID_FILE -l /var/lock/subsys/$prog $prog
        RETVAL=$?
        ;;
  restart|reload)
        stop
        start
        ;;
  condrestart)
        [ -f /var/lock/subsys/$prog ] && stop && start || :
        ;;
  *)
        echo $"Usage: $0 {start|stop|status|restart|reload|condrestart}"
        RETVAL=1
esac
exit $RETVAL
