//
//  FakeAnomalyAlgorithm.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import org.kairosdb.core.DataPoint;

public class FakeAnomalyAlgorithm implements AnomalyAlgorithm
{
	@Override
	public boolean isAnomaly(String metricName, DataPoint dataPoint)
	{
		return true;
	}
}