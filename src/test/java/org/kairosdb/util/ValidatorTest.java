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

import com.google.gson.*;
import org.junit.Test;
import org.kairosdb.core.http.rest.json.ValidationErrors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ValidatorTest
{
	@Test
	public void test_validateCharacterSet_valid() throws ValidationException
	{
		Validator.validateCharacterSet("test", "ABC-123_xyz/456.789");
	}

	@Test
	public void test_validateCharacterSet_invalid() throws ValidationException
	{
		try
		{
			Validator.validateCharacterSet("test", "abc:123");
			fail("Expected ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("test may contain any character except colon ':', and equals '='."));
		}
	}

	@Test
	public void test_validateNotNullOrEmpty_valid() throws ValidationException
	{
		Validator.validateNotNullOrEmpty("name", "the name");
	}

	@Test
	public void test_validateNotNullOrEmpty_empty_invalid() throws ValidationException
	{
		try
		{
			Validator.validateNotNullOrEmpty("name", "");
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
			Validator.validateNotNullOrEmpty("name", null);
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
		Validator.validateMin("name", 2, 1);
	}

	@Test
	public void test_validateMin_invalid() throws ValidationException
	{
		try
		{
			Validator.validateMin("name", 10, 11);
			fail("Expected ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("name must be greater than or equal to 11."));
		}
	}

	@Test
	public void test_isValidateCharacterSet_valid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isValidateCharacterSet(errors, "test", "ABC-123_xyz/456.789"), equalTo(true));
		assertThat(errors.size(), equalTo(0));
	}

	@Test
	public void test_isValidateCharacterSet_invalid() throws ValidationException
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isValidateCharacterSet(errors, "test", "ABC:123"), equalTo(false));
		assertThat(errors.getErrors(), hasItem("test may contain any character except colon ':', and equals '='."));
	}

	@Test
	public void test_isNotNullOrEmpty_string_valid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isNotNullOrEmpty(errors, "name", "the name"), equalTo(true));
	}

	@Test
	public void test_isNotNullOrEmpty_string_invalid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isNotNullOrEmpty(errors, "name", (String)null), equalTo(false));
		assertThat(errors.getErrors(), hasItem("name may not be null."));
	}


	@Test
	public void test_isNotNullOrEmpty_JsonElement_null_value_invalid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isNotNullOrEmpty(errors, "value", (JsonElement)null), equalTo(false));
		assertThat(errors.getErrors(), hasItem("value may not be null."));
	}

	@Test
	public void test_isNotNullOrEmpty_JsonElement_isJsonNull_value_invalid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isNotNullOrEmpty(errors, "value", JsonNull.INSTANCE), equalTo(false));
		assertThat(errors.getErrors(), hasItem("value may not be empty."));
	}

	@Test
	public void test_isNotNullOrEmpty_JsonPrimitive_empty_value_invalid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isNotNullOrEmpty(errors, "value", new JsonPrimitive("")), equalTo(false));
		assertThat(errors.getErrors(), hasItem("value may not be empty."));
	}

	@Test
	public void test_isNotNullOrEmpty_JsonArray_empty_value_invalid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isNotNullOrEmpty(errors, "value", new JsonArray()), equalTo(false));
		assertThat(errors.getErrors(), hasItem("value may not be an empty array."));
	}

	@Test
	public void test_isNotNullOrEmpty_JsonObject_empty_value_valid()
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isNotNullOrEmpty(errors, "value", new JsonObject()), equalTo(true));
		assertThat(errors.getErrors().size(), equalTo(0));
	}

	@Test
	public void test_isGreaterThanOrEqualTo_valid() throws ValidationException
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isGreaterThanOrEqualTo(errors, "name", 2, 1), equalTo(true));
	}

	@Test
	public void test_isGreaterThanOrEqualTo_invalid() throws ValidationException
	{
		ValidationErrors errors = new ValidationErrors();

		assertThat(Validator.isGreaterThanOrEqualTo(errors, "name", 10, 11), equalTo(false));
		assertThat(errors.getErrors(), hasItem("name must be greater than or equal to 11."));
	}
}