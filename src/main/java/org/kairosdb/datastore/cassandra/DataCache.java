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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 Used to keep a limited size cache in memory.  The data type must implement
 hashcode and equal methods.
 */
public class DataCache<T>
{
	private Object m_lock = new Object();

	private class InternalCache extends LinkedHashMap<T, String>
	{
		private int m_cacheSize;

		public InternalCache(int cacheSize)
		{
			super(16, (float)0.75, true);
			m_cacheSize = cacheSize;
		}

		protected boolean removeEldestEntry(Map.Entry<T, String> entry)
		{
			return (size() > m_cacheSize);
		}
	}

	private InternalCache m_cache;

	public DataCache(int cacheSize)
	{
		m_cache = new InternalCache(cacheSize);
	}

	/**
	 Returns true if the item is already in the cache.  If the item is not
	 in the cache the item is added.
	 @param cacheData Item to check if in cache and or to insert into cache
	 @return  Returns true if in cache, false otherwise.
	 */
	public boolean isCached(T cacheData)
	{
		String ret;

		synchronized (m_lock)
		{
			ret = m_cache.put(cacheData, "");
		}

		return (ret != null);
	}


}
