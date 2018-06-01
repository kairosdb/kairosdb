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

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DataCacheTest
{
	public class TestObject
	{
		private final String m_data;

		public TestObject(String data)
		{
			m_data = data;
		}

		@Override
		public boolean equals(Object o)
		{
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestObject that = (TestObject) o;

			if (!m_data.equals(that.m_data)) return false;

			return true;
		}

		@Override
		public int hashCode()
		{
			return m_data.hashCode();
		}
	}
	@Test
	public void test_isCached()
	{
		DataCache<String> cache = new DataCache<String>(3);

		assertNull(cache.getItem("one"));
		assertNull(cache.cacheItem("one"));
		assertNotNull(cache.getItem("one"));

		assertNull(cache.getItem("two"));
		assertNull(cache.cacheItem("two"));
		assertNotNull(cache.getItem("two"));

		assertNull(cache.getItem("three"));
		assertNull(cache.cacheItem("three"));
		assertNotNull(cache.getItem("three"));

		assertNotNull(cache.cacheItem("one")); //This puts 'one' as the newest
		assertNull(cache.cacheItem("four")); //This should boot out 'two'
		assertNull(cache.cacheItem("two")); //Should have booted 'three'
		assertNotNull(cache.cacheItem("one"));
		assertNull(cache.getItem("three"));
		assertNull(cache.cacheItem("three")); //Should have booted 'four'
		assertNotNull(cache.cacheItem("one"));
	}

	@Test
	public void test_uniqueCache()
	{
		TestObject td1 = new TestObject("td1");
		TestObject td2 = new TestObject("td2");
		TestObject td3 = new TestObject("td3");

		DataCache<TestObject> cache = new DataCache<TestObject>(10);

		cache.cacheItem(td1);
		cache.cacheItem(td2);
		cache.cacheItem(td3);

		TestObject ret = cache.cacheItem(new TestObject("td1"));
		assertTrue(td1 == ret);

		ret = cache.cacheItem(new TestObject("td2"));
		assertTrue(td2 == ret);

		ret = cache.cacheItem(new TestObject("td3"));
		assertTrue(td3 == ret);
	}
}
