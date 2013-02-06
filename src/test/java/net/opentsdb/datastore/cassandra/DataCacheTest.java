// OpenTSDB2
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

package net.opentsdb.datastore.cassandra;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class DataCacheTest
{
	@Test
	public void test_isCached()
	{
		DataCache<String> cache = new DataCache<String>(3);

		assertFalse(cache.isCached("one"));
		assertFalse(cache.isCached("two"));
		assertFalse(cache.isCached("three"));

		assertTrue(cache.isCached("one")); //This puts 'one' as the newest
		assertFalse(cache.isCached("four")); //This should boot out 'two'
		assertFalse(cache.isCached("two")); //Should have booted 'three'
		assertTrue(cache.isCached("one"));
		assertFalse(cache.isCached("three")); //Should have booted 'four'
		assertTrue(cache.isCached("one"));
	}
}
