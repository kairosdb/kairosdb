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
package org.kairosdb.core.http.rest.json;

import org.kairosdb.testing.BeanValidationHelper;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SamplingTest
{
	@Test
	public void testDurationZeroInvalid()
	{
		Sampling sampling = new Sampling(0, "days", "sum");
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(sampling);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("duration must be greater than or equal to 1"));
	}

	@Test
	public void testUnitNullInvalid()
	{
		Sampling sampling = new Sampling(1, null, "sum");
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(sampling);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("unit must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}

	@Test
	public void testUnitEmptyInvalid()
	{
		Sampling sampling = new Sampling(1, "", "sum");
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(sampling);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("unit must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}

	@Test
	public void testUnitInvalidUnit()
	{
		Sampling request = new Sampling(5, "foo", "sum");
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(request);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("unit must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}

	@Test
	public void testAggregatorNullInvalid()
	{
		Sampling sampling = new Sampling(1, "days", null);
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(sampling);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("aggregate may not be null"));
	}

	@Test
	public void testAggregatorEmptyInvalid()
	{
		Sampling sampling = new Sampling(1, "days", "");
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(sampling);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("aggregate may not be empty"));
	}
}