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
package org.kairosdb.core.http.rest.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;

public class ValidMapValidator implements ConstraintValidator<ValidMapRequired, Map<String, String>>
{
	@Override
	public void initialize(ValidMapRequired validMapRequired)
	{
	}

	@Override
	public boolean isValid(Map<String, String> map, ConstraintValidatorContext context)
	{
		for (String key : map.keySet())
		{
			if (key == null || key.isEmpty())
			{
				context.disableDefaultConstraintViolation(); // disable violation message
				context.buildConstraintViolationWithTemplate("key value cannot be null or empty").addConstraintViolation();  // add message
				return false;
			}
			if (map.get(key) == null || map.get(key).isEmpty())
			{
				context.disableDefaultConstraintViolation(); // disable violation message
				context.buildConstraintViolationWithTemplate("value cannot be null or empty for key: " + key).addConstraintViolation();  // add message
				return false;
			}
		}

		return true;
	}
}