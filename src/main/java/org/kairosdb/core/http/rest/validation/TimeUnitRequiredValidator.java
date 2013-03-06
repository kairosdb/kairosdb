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