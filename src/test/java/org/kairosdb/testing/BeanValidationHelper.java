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
package org.kairosdb.testing;

import com.google.common.collect.ImmutableList;
import org.apache.bval.jsr303.ApacheValidationProvider;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Collection;
import java.util.List;

public class BeanValidationHelper
{
	public static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

	public static List<String> messagesFor(Collection<? extends ConstraintViolation<?>> violations)
	{
		ImmutableList.Builder<String> messages = new ImmutableList.Builder<String>();
		for (ConstraintViolation<?> violation : violations)
		{
			messages.add(violation.getPropertyPath().toString() + " " + violation.getMessage());
		}

		return messages.build();
	}

}