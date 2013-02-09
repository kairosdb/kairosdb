package net.opentsdb.core;

import net.opentsdb.core.exception.TsdbException;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 2/8/13
 Time: 6:41 PM
 To change this template use File | Settings | File Templates.
 */
public interface OpenTsdbService
{
	public void start() throws TsdbException;
	public void stop();
}
