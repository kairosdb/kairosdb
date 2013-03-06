// KairosDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>

package org.kairosdb.datastore.cassandra;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 Used to keep a limited size cache in memory.  The data type must implement
 hashcode and equal methods.
 */
public class DataCache<T>
{
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
		String ret = m_cache.put(cacheData, "");

		return (ret != null);
	}


}
