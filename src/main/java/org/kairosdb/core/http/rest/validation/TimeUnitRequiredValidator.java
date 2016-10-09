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

import org.kairosdb.core.datastore.TimeUnit;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class TimeUnitRequiredValidator implements ConstraintValidator<TimeUnitRequired, String>
{
    public void initialize(TimeUnitRequired constraintAnnotation)
    {
    }

    public boolean isValid(String value, ConstraintValidatorContext context)
    {
        if (!TimeUnit.contains(value))
        {
            context.disableDefaultConstraintViolation(); // disable violation message
            context.buildConstraintViolationWithTemplate("must be one of " + TimeUnit.toValueNames()).addConstraintViolation();  // add message
            return false;
        }

        return true;
    }
}