//
//  AnomalyModule.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import com.google.inject.AbstractModule;

public class AnomalyModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(AnomalyModule.class).in(Singleton.class);

	}
}