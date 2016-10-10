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