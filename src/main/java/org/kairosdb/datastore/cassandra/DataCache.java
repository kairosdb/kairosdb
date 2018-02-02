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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
	private final Object m_lock = new Object();

	private final LinkItem<T> m_front = new LinkItem<T>(null);
	private final LinkItem<T> m_back = new LinkItem<T>(null);

	private int m_maxSize;


	private class LinkItem<T>
	{
		private LinkItem<T> m_prev;
		private LinkItem<T> m_next;

		private final T m_data;

		public LinkItem(T data)
		{
			m_data = data;
		}
	}

	//Using a ConcurrentHashMap so we can use the putIfAbsent method.
	private ConcurrentHashMap<T, LinkItem<T>> m_hashMap;

	public DataCache(int cacheSize)
	{
		//m_cache = new InternalCache(cacheSize);
		m_hashMap = new ConcurrentHashMap<>();
		m_maxSize = cacheSize;

		m_front.m_next = m_back;
		m_back.m_prev = m_front;
	}

	/**
	 returns null if item was not in cache.  If the return is not null the item
	 from the cache is returned.

	 @param cacheData
	 @return
	 */
	public T cacheItem(T cacheData)
	{
		LinkItem<T> mappedItem = null;

		synchronized (m_lock)
		{
			LinkItem<T> li = new LinkItem<T>(cacheData);
			mappedItem = m_hashMap.putIfAbsent(cacheData, li);

			if (mappedItem != null)
			{
				//moves item to top of list
				remove(mappedItem);
				addItem(mappedItem);
			}
			else
				addItem(li);

			if (m_hashMap.size() > m_maxSize)
			{
				LinkItem<T> last = m_back.m_prev;
				remove(last);

				m_hashMap.remove(last.m_data);
			}
		}

		return (mappedItem == null ? null : mappedItem.m_data);
	}

	private void remove(LinkItem<T> li)
	{
		li.m_prev.m_next = li.m_next;
		li.m_next.m_prev = li.m_prev;
	}

	private void addItem(LinkItem<T> li)
	{
		li.m_prev = m_front;
		li.m_next = m_front.m_next;

		m_front.m_next = li;
		li.m_next.m_prev = li;
	}

	public Set<T> getCachedKeys()
	{
		return (m_hashMap.keySet());
	}

	public void removeKey(T key)
	{
		synchronized (m_lock)
		{
			LinkItem<T> li = m_hashMap.remove(key);
			if (li != null)
				remove(li);
		}
	}

	public void clear()
	{
		synchronized (m_lock)
		{
			m_front.m_next = m_back;
			m_back.m_prev = m_front;

			m_hashMap.clear();
		}
	}
}
