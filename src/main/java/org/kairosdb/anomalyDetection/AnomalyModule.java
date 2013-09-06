//
//  AnomalyModule.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class AnomalyModule extends AbstractModule
{
	@Override
	protected void configure()
	{
//		bind(SkylineDataPointFeed.class).in(Singleton.class);
		bind(AnomalyDetector.class).in(Singleton.class);
		bind(StdDevFromAverageAlgorithm.class).in(Singleton.class);
	}
}