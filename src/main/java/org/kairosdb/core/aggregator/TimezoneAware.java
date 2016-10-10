package org.kairosdb.core.aggregator;

import org.joda.time.DateTimeZone;

/**
 I don't like this way of doing it but, until I can figure out how to make guice
 do it this is the best I got.
 Created by bhawkins on 6/4/15.
 */
public interface TimezoneAware
{
	void setTimeZone(DateTimeZone timeZone);
}
