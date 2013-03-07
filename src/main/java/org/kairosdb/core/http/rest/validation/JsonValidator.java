// KairosDB2
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

package org.kairosdb.core.http.rest.validation;

public class JsonValidator
{
	private JsonValidator()
	{
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
			throw new ValidationException(name + " must be greater than or equal to " + minValue);
	}

}