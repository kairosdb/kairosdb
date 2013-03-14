#!/bin/bash
#
# chkconfig: 35 90 12
# description: KairosDB2 server
#
# Get function from functions library
. /etc/init.d/functions
# Start the service KairosDB2

#Set JAVA_HOME if your java is not in the path already
#export JAVA_HOME=/etc/alternatives/jre

start() {
        initlog -c "echo -n Starting KairosDB server: "
        /opt/kairosdb/bin/kairosdb.sh start
        ### Create the lock file ###
        touch /var/lock/subsys/KairosDB2
        success $"KairosDB2 server startup"
        echo
}

# Restart the service KairosDB2
stop() {
        initlog -c "echo -n Stopping KairosDB server: "
        /opt/kairosdb/bin/kairosdb.sh stop
        ### Now, delete the lock file ###
        rm -f /var/lock/subsys/KairosDB2
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
        status KairosDB2
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
