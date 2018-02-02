#!/bin/bash

if ! type chkconfig &> /dev/null; then
	update-rc.d kairosdb defaults
else
	chkconfig --add kairosdb
	chkconfig kairosdb on
fi

