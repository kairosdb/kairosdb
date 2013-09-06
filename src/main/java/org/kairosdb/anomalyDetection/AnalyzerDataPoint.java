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

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/6/13
 Time: 2:14 PM
 To change this template use File | Settings | File Templates.
 */
public class AnalyzerDataPoint implements Comparable<AnalyzerDataPoint>
{
	private long m_timestamp;
	private double m_value;


	public AnalyzerDataPoint(long timestamp, double value)
	{
		m_timestamp = timestamp;
		m_value = value;
	}

	public long getTimestamp() { return m_timestamp; }
	public double getValue() { return m_value; }


	@Override
	public int compareTo(AnalyzerDataPoint o)
	{
		double ret = m_value - o.m_value;

		if (ret == 0.0)
		{
			return (int)(m_timestamp - o.m_timestamp);
		}
		else
		{
			return (ret < 0.0 ? -1 : 1);
		}
	}
}
