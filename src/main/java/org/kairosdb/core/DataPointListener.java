//
// DataPointListener.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core;

import java.util.SortedMap;

/**
 Any implementations that take a long time should use a separate thread
 to do any work.  The calling thread is the protocol thread.
 */
public interface DataPointListener
{
	public void dataPoint(String metricName, SortedMap<String, String> tags, DataPoint dataPoint);
}
