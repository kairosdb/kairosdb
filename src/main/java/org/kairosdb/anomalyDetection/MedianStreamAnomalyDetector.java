/*
 * Copyright 2013 Proofpoint Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.anomalyDetection;


import org.kairosdb.core.DataPoint;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/6/13
 Time: 1:17 PM
 To change this template use File | Settings | File Templates.
 */
public class MedianStreamAnomalyDetector implements AnomalyAlgorithm
{
	private static final int TREE_SIZE = 20;
	private static final double FACTOR = 1.5;

	private AnalyzerTreeMap<AnalyzerDataPoint, String> m_tree;

	public MedianStreamAnomalyDetector()
	{
		m_tree = new AnalyzerTreeMap<AnalyzerDataPoint, String>(TREE_SIZE);
	}

	@Override
	public boolean isAnomaly(String metricName, DataPoint dataPoint)
	{
		boolean anomaly = false;

		if (m_tree.size() == TREE_SIZE)
		{
			double low = m_tree.firstKey().getValue();
			double med = m_tree.getRootKey().getValue();
			double hi = m_tree.lastKey().getValue();

			double range = Math.max(med - low, hi - med);
			range *= FACTOR;

			if (Math.abs(med - dataPoint.getDoubleValue()) > range)
			{
				anomaly = true;
			}
		}

		m_tree.put(new AnalyzerDataPoint(dataPoint.getTimestamp(), dataPoint.getDoubleValue()), null);

		return anomaly;
	}
}
