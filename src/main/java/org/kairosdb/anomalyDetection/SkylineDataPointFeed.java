//
//  SkylineDataPointFeed.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;

public class SkylineDataPointFeed implements DataPointListener
{
	private static final String SKYLINE_SERVER = "10.92.0.10";


	@Override
	public void dataPoints(DataPointSet pds)
	{
	}
}