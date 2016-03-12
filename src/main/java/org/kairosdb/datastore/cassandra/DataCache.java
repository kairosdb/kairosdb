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

import com.google.common.base.Predicate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

	private final LinkItem<T> m_front = new LinkItem<>(null);
	private final LinkItem<T> m_back = new LinkItem<>(null);

	private final int m_maxSize;

	private static class LinkItem<T>
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
	private final Map<T, LinkItem<T>> m_hashMap = new HashMap<>();

	public DataCache(int cacheSize)
	{
		m_maxSize = cacheSize;

		m_front.m_next = m_back;
		m_back.m_prev = m_front;
	}

	/**
	 returns null if item was not in cache.  If the return is not null the item
	 from the cache is returned.

	 @param key
	 @return
	 */
	public T cacheItem(T key)
	{
		synchronized (m_lock)
		{
			final LinkItem<T> cachedItem = m_hashMap.get(key);
			if (cachedItem != null)
			{
				remove(cachedItem);
				addItem(cachedItem);
			}
			else
			{
				final LinkItem<T> linkItem = new LinkItem<>(key);
				m_hashMap.put(key, linkItem);
				addItem(linkItem);
			}

			trim();
			return cachedItem != null ? cachedItem.m_data : null;
		}

	}

	private void trim() {
		while (m_hashMap.size() > m_maxSize)
		{
			LinkItem<T> last = m_back.m_prev;
			remove(last);

			m_hashMap.remove(last.m_data);
		}
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

	public void removeIf(Predicate<T> predicate) {
		synchronized (m_lock) {
			final Iterator<Map.Entry<T, LinkItem<T>>> iterator = m_hashMap.entrySet().iterator();
			while (iterator.hasNext()) {
				final Map.Entry<T, LinkItem<T>> next = iterator.next();
				if (predicate.apply(next.getKey())) {
					iterator.remove();
					remove(next.getValue());
				}
			}
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
