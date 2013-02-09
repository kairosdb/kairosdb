#!/bin/bash
#
# chkconfig: 35 90 12
# description: OpenTSDB2 server
#
# Get function from functions library
. /etc/init.d/functions
# Start the service OpenTSDB2

start() {
        initlog -c "echo -n Starting OpenTSDB2 server: "
        /opt/opentsdb2/bin/opentsdb2.sh start
        ### Create the lock file ###
        touch /var/lock/subsys/OpenTSDB2
        success $"OpenTSDB2 server startup"
        echo
}

# Restart the service OpenTSDB2
stop() {
        initlog -c "echo -n Stopping OpenTSDB2 server: "
        /opt/opentsdb2/bin/opentsdb2.sh stop
        ### Now, delete the lock file ###
        rm -f /var/lock/subsys/OpenTSDB2
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
        status OpenTSDB2
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
