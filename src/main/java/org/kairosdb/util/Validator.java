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

import org.kairosdb.core.http.rest.json.ValidationErrors;

public class Validator
{
	private Validator()
	{
	}

	public static void validateCharacterSet(String name, String value) throws ValidationException
	{
		ValidationErrors errors = new ValidationErrors();
		if (!isValidateCharacterSet(errors, name, value))
			throw new ValidationException(errors.getFirstError());
	}

	public static void validateNotNullOrEmpty(String name, String value) throws ValidationException
	{
		ValidationErrors errors = new ValidationErrors();
		if (!isNotNullOrEmpty(errors, name, value))
			throw new ValidationException(errors.getFirstError());
	}

	public static void validateMin(String name, long value, long minValue) throws ValidationException
	{
		ValidationErrors errors = new ValidationErrors();
		if (!isGreaterThanOrEqualTo(errors, name, value, minValue))
			throw new ValidationException(errors.getFirstError());
	}

	public static boolean isValidateCharacterSet(ValidationErrors validationErrors, String name, String value)
	{
		if (value == null || value.isEmpty() || !CharacterSet.isValid(value)){
			validationErrors.addErrorMessage(name + " may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'.");
			return false;
		}
		return true;
	}

	public static boolean isNotNullOrEmpty(ValidationErrors validationErrors, String name, String value)
	{
		if (value == null)
		{
			validationErrors.addErrorMessage(name + " may not be null.");
			return false;
		}
		if (value.isEmpty())
		{
			validationErrors.addErrorMessage(name + " may not be empty.");
			return false;
		}

		return true;
	}

	public static boolean isGreaterThanOrEqualTo(ValidationErrors validationErrors, String name, long value, long minValue)
	{
		if (value < minValue)
		{
			validationErrors.addErrorMessage(name + " must be greater than or equal to " + minValue + ".");
			return false;
		}
		return true;
	}

}