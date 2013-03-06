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