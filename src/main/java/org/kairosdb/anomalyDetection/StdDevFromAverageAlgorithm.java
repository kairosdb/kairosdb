//
//  StdDevFromAverageAlgorithm.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import org.kairosdb.core.DataPoint;

import java.util.HashMap;
import java.util.Map;

public class StdDevFromAverageAlgorithm implements AnomalyAlgorithm
{
	private Map<String, DataPointAverage> averageMap = new HashMap<String, DataPointAverage>();

	@Override
	public double isAnomaly(String metricName, DataPoint dataPoint)
	{
		if (!metricName.equals("jeff_test"))
			return 0;

		DataPointAverage average;
		if (averageMap.containsKey(metricName))
			average = averageMap.get(metricName);
		else
		{
			average = new DataPointAverage();
			averageMap.put(metricName, average);
		}

		average.incrementCount();
		average.incrementAverage((dataPoint.getDoubleValue() - average.average) / average.count);
		average.incrementPwrSumAvg((dataPoint.getDoubleValue() * dataPoint.getDoubleValue() - average.pwrSumAvg) / average.count);
		double stdDev = Math.sqrt((average.pwrSumAvg * average.count - average.count * average.average * average.average) / (average.count - 1));

		System.out.println("Average: " + average.average + " stddev = " + stdDev);

		// Anomaly if 3 stddev from average
		if(dataPoint.getDoubleValue() > (average.average + 3 * stdDev) ||
				dataPoint.getDoubleValue() < (average.average - 3 * stdDev))
			return 1;
		else
			return 0;
	}

	private class DataPointAverage
	{
		private double average;
		private int count;
		private double pwrSumAvg;

		private void incrementAverage(double average)
		{
			this.average += average;
		}

		private void incrementCount()
		{
			this.count++;
		}

		private void incrementPwrSumAvg(double value)
		{
			pwrSumAvg += value;
		}
	}
}