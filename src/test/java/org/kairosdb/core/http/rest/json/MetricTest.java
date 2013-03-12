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

public class MetricTest
{
	@Test
	public void testUnitNullInvalid()
	{
		Metric metric = new Metric(null, "sum", null, true, null, null);
		Set<ConstraintViolation<Metric>> violations = BeanValidationHelper.VALIDATOR.validate(metric);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("name may not be null"));
	}

	@Test
	public void testUnitEmptyInvalid()
	{
		Metric metric = new Metric("", "sum", null, true, null, null);
		Set<ConstraintViolation<Metric>> violations = BeanValidationHelper.VALIDATOR.validate(metric);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("name may not be empty"));
	}

	@Test
	public void testAggregateNullInValid()
	{
		Metric metric = new Metric("name", null, null, true, null, null);
		Set<ConstraintViolation<Metric>> violations = BeanValidationHelper.VALIDATOR.validate(metric);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("aggregate may not be null"));
	}

	@Test
	public void testAggregateEmptyInValid()
	{
		Metric metric = new Metric("name", "", null, true, null, null);
		Set<ConstraintViolation<Metric>> violations = BeanValidationHelper.VALIDATOR.validate(metric);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("aggregate may not be empty"));
	}
}