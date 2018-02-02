/*
 * Copyright 2016 KairosDB Authors
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

package org.kairosdb.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.concurrent.atomic.AtomicInteger;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/16/13
 Time: 2:21 PM
 To change this template use File | Settings | File Templates.
 */
public class MemoryMonitor
{
	private static final MemoryPoolMXBean s_pool = findPool();

	private static MemoryPoolMXBean findPool()
	{
		MemoryPoolMXBean ret = null;
		for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
			if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
				ret = pool;
			}
		}
		// we do something when we reached 99.9% of memory usage
		// when we get to this point gc was unable to recover memory.
		ret.setCollectionUsageThreshold((long) Math.floor(ret.getUsage().getMax() * 0.999));

		return (ret);
	}

	private int m_checkRate;
	private AtomicInteger m_checkCounter;

	public MemoryMonitor()
	{
		this(1);
	}

	public MemoryMonitor(int checkRate)
	{
		m_checkRate = checkRate;
		m_checkCounter = new AtomicInteger();
	}

	public void setCheckRate(int checkRate)
	{
		m_checkRate = checkRate;
	}

	public boolean isMemoryLow()
	{
		int value = m_checkCounter.incrementAndGet();

		if (value % m_checkRate == 0)
			return (s_pool.isCollectionUsageThresholdExceeded());
		else
			return (false);
	}

	public void checkMemoryAndThrowException()
	{
		if (isMemoryLow())
		{
			throw new MemoryMonitorException();
		}
	}
}
