/*
 * Copyright 2016 KairosDB Authors
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

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 This cache serves two purposes.
 1.  Cache recently inserted data
 2.  Create a unique store of cached data.

 The primary use of this class is to store row keys so we know if the row key
 index needs to be updated or not.  Because it uniquely stores row keys we
 can use the same row key object over and over.  With row keys we store the
 serialized form of the key so we only have to serialize a row key once.

 The data type must implement hashcode and equal methods.
 */
public class DataCache<T>
{
	private Map<T, T> m_internalMap;

	public DataCache(final int cacheSize)
	{
		m_internalMap = Collections.synchronizedMap(new LinkedHashMap<T, T>(cacheSize, 1f, true) {
					@Override
					protected boolean removeEldestEntry(Map.Entry<T, T> eldest)
					{
						return size() > cacheSize;
					}
				});
	}

	/**
	 returns null if item was not in cache.  If the return is not null the item
	 from the cache is returned.

	 @param cacheData
	 @return
	 */
	public T cacheItem(T cacheData)
	{
		return m_internalMap.put(cacheData, cacheData);
	}


	public Set<T> getCachedKeys()
	{
		synchronized (m_internalMap)
		{
			return ImmutableSet.copyOf(m_internalMap.keySet());
		}
	}

	public void removeKey(T key)
	{
		m_internalMap.remove(key);
	}

	public void clear()
	{
		m_internalMap.clear();
	}
}
