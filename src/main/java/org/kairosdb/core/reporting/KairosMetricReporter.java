//
// KairosMetricReporter.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.reporting;

import org.kairosdb.core.DataPointSet;

import java.util.List;

public interface KairosMetricReporter
{
	public List<DataPointSet> getMetrics(long now);
}
