============
Logging
============

KairosDB creates its log files in ``/opt/kairosdb/log/``.  Log rotation works on a daily
basis, so every day a new log is created and named as **kairosdb.log**; when the
next day comes *kairosdb.log* is compressed and renamed to *kairosdb.<YYYY-MM-DD>.log*,
where <YYYY-MM-DD> is the date when this specific log file was created.

Starting from KairosDB 1.1.2, a more robust logging system will be used.
From this version, KairosDB will compress logs everyday in order to minimize
the space used for log files. Also, every time a log file reaches 100mb, it
will be compressed and a new one will be generated for the same day (until the
end of the day, when a new log file will be generated too).

	**Note:** KairosDB uses the well-known Logback framework to manage its log
	files. If you have questions about Logback, please refer to their forums.

If you want to change these logging settings, please take a look at
``/opt/kairosdb/conf/logging/logback.xml`` and at `Logback documentation`_.

.. _Logback documentation: http://logback.qos.ch/manual/configuration.html