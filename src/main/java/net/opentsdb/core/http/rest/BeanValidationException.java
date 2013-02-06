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
package net.opentsdb.core.http.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.validation.ConstraintViolation;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Thrown when bean validation has errors.
 */
public class BeanValidationException extends IOException
{
	private ImmutableSet<ConstraintViolation<Object>> violations;

	public BeanValidationException(Set<ConstraintViolation<Object>> violations)
	{
		super(messagesFor(violations).toString());
		this.violations = ImmutableSet.copyOf(violations);
	}

	/**
	 * Returns the bean validation error messages.
	 *
	 * @return validation error messages
	 */
	public List<String> getErrorMessages()
	{
		return messagesFor(violations);
	}

	/**
	 * Returns the set of bean validation violations.
	 *
	 * @return set of bean validation violations
	 */
	public Set<ConstraintViolation<Object>> getViolations()
	{
		return violations;
	}

	private static List<String> messagesFor(Set<ConstraintViolation<Object>> violations)
	{
		ImmutableList.Builder<String> messages = new ImmutableList.Builder<String>();
		for (ConstraintViolation<?> violation : violations) {
			messages.add(violation.getPropertyPath().toString() + " " + violation.getMessage());
		}

		return messages.build();
	}
}