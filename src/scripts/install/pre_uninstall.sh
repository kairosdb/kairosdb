#!/bin/bash

if ! type chkconfig &> /dev/null; then
	update-rc.d -f kairosdb remove
else
	chkconfig kairosdb off
	chkconfig --del kairosdb
fi
