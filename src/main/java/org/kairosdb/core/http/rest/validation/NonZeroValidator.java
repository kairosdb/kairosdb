//
//  NonZeroValidator.java
//
// Copyright 2016, KairosDB Authors
//        
package org.kairosdb.core.http.rest.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class NonZeroValidator implements ConstraintValidator<NonZero, Double>
{
	@Override
	public void initialize(NonZero nonZero)
	{
	}

	@Override
	public boolean isValid(Double aDouble, ConstraintValidatorContext context)
	{
		if (aDouble == 0)
		{
			context.disableDefaultConstraintViolation(); // disable violation message
			context.buildConstraintViolationWithTemplate("may not be zero").addConstraintViolation();  // add message
			return false;
		}

		return true;
	}
}