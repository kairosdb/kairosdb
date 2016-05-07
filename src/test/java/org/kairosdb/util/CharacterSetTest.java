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
package org.kairosdb.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class CharacterSetTest
{

	@Test
	public void test_isValid_valid()
	{
		assertTrue(CharacterSet.isValidTagNameValue("ABC-123_xyz/456.789"));
	}

	@Test
	public void test_isValid_colon_invalid()
	{
		assertFalse(CharacterSet.isValidTagNameValue("abc:123"));
	}

	@Test
	public void test_isValid_equal_invalid()
	{
		assertFalse(CharacterSet.isValidTagNameValue("abc=123"));
	}

	@Test
	public void test_isValid_backslash_valid()
	{
		assertTrue(CharacterSet.isValidTagNameValue("abc\\123"));
	}

	@Test
	public void test_isValid_space_valid()
	{
		assertTrue(CharacterSet.isValidTagNameValue("abc 123"));
	}

	@Test
	public void test_isValid_tab_valid()
	{
		assertTrue(CharacterSet.isValidTagNameValue("abc   123"));
	}

}