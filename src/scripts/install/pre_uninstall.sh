#!/bin/bash

/etc/init.d/kairosdb stop

if ! type chkconfig &> /dev/null; then
	update-rc.d -f kairosdb remove
else
	chkconfig kairosdb off
	chkconfig --del kairosdb
fi
