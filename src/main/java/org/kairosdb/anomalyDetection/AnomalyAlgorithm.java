//
//  AnomalyAlgorithm.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import org.kairosdb.core.DataPoint;

public interface AnomalyAlgorithm
{
	boolean isAnomaly(String metricName, DataPoint dataPoint);
}