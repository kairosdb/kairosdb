/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.datastore.cassandra;

public class LongOrDouble
{
	private boolean m_isLong;
	private long m_longValue;
	private double m_doubleValue;

	public LongOrDouble(long longValue)
	{
		m_isLong = true;
		m_longValue = longValue;
	}

	public LongOrDouble(double doubleValue)
	{
		m_isLong = false;
		m_doubleValue = doubleValue;
	}

	public boolean isLong()
	{
		return m_isLong;
	}

	public long getLongValue()
	{
		return m_longValue;
	}

	public double getDoubleValue()
	{
		return m_doubleValue;
	}
}
