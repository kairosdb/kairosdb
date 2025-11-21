package org.kairosdb.congestion;

/**
 Created by bhawkins on 3/19/16.
 */
public interface ProcessTracker
{
	void finished();
	void failed();
}
