//
//  Anomaly.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import org.kairosdb.core.DataPoint;

public class Anomaly
{
	private String metricName;
	private DataPoint datapoint;
	private double score;

	public Anomaly(String metricName, DataPoint datapoint, double score)
	{
		this.metricName = metricName;
		this.datapoint = datapoint;
		this.score = score;

	}

	public String getMetricName()
	{
		return metricName;
	}

	public DataPoint getDatapoint()
	{
		return datapoint;
	}

	public double getScore()
	{
		return score;
	}

	public long getTimestamp()
	{
		return datapoint.getTimestamp();
	}
}