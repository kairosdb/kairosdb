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

package org.kairosdb.core.carbon.pickle;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/7/13
 Time: 12:48 PM
 To change this template use File | Settings | File Templates.
 */
public class PickleMetric
{
	private String m_path;
	private long m_time;
	private boolean m_isLong;
	private long m_longValue;
	private double m_doubleValue;

	public PickleMetric(String path)
	{
		m_path = path;
	}

	public PickleMetric(String path, long time, long value)
	{
		m_isLong = true;
		m_path = path;
		m_time = time;
		m_longValue = value;
	}

	public PickleMetric(String path, long time, double value)
	{
		m_isLong = false;
		m_path = path;
		m_time = time;
		m_doubleValue = value;
	}

	public void setTime(long time) { m_time = time; }
	public void setValue(long value)
	{
		m_isLong = true;
		m_longValue = value;
	}

	public void setValue(double value)
	{
		m_isLong = false;
		m_doubleValue = value;
	}

	public boolean isLongValue() { return m_isLong; }

	public String getPath() { return m_path; }
	public long getTime() { return m_time; }

	public long getLongValue() { return m_longValue; }
	public double getDoubleValue() { return m_doubleValue; }
}
