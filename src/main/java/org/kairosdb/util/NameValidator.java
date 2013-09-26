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

public class NameValidator
{
	private NameValidator()
	{
	}

	public static void validateCharacterSet(String name, String value) throws ValidationException
	{
		if (!CharacterSet.isValid(value))
			throw new ValidationException(name + " may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'.");
	}

	public static void validateNotNullOrEmpty(String name, String value) throws ValidationException
	{
		if (value == null)
			throw new ValidationException(name + " may not be null.");
		if (value.isEmpty())
			throw new ValidationException(name + " may not be empty.");
	}

	public static void validateMin(String name, long value, long minValue) throws ValidationException
	{
		if (value < minValue)
			throw new ValidationException(name + " must be greater than or equal to " + minValue + ".");
	}

}