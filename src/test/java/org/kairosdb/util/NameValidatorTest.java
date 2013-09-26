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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class NameValidatorTest
{
	@Test
	public void test_validateCharacterSet_valid() throws ValidationException
	{
		NameValidator.validateCharacterSet("test", "ABC-123_xyz/456.789");
	}

	@Test
	public void test_validateCharacterSet_invalid() throws ValidationException
	{
		try
		{
			NameValidator.validateCharacterSet("test", "abc:123");
			fail("Expected ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("test may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'."));
		}
	}

	@Test
	public void test_validateNotNullOrEmpty_valid() throws ValidationException
	{
		NameValidator.validateNotNullOrEmpty("name", "the name");
	}

	@Test
	public void test_validateNotNullOrEmpty_empty_invalid() throws ValidationException
	{
		try
		{
			NameValidator.validateNotNullOrEmpty("name", "");
			fail("Expected ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("name may not be empty."));
		}
	}

	@Test
	public void test_validateNotNullOrEmpty_null_invalid() throws ValidationException
	{
		try
		{
			NameValidator.validateNotNullOrEmpty("name", null);
			fail("Expected ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("name may not be null."));
		}
	}

	@Test
	public void test_validateMin_valid() throws ValidationException
	{
		NameValidator.validateMin("name", 2, 1);
	}

	@Test
	public void test_validateMin_invalid() throws ValidationException
	{
		try
		{
			NameValidator.validateMin("name", 10, 11);
			fail("Expected ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("name must be greater than or equal to 11."));
		}
	}
}