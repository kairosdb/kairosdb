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
package net.opentsdb.core.http.rest.json;

import net.opentsdb.testing.BeanValidationHelper;
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
		assertThat(violationMessages.get(0), equalTo("unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}

	@Test
	public void testUnitEmptyInvalid()
	{
		Sampling sampling = new Sampling(1, "", "sum");
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(sampling);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}

	@Test
	public void testUnitInvalidUnit()
	{
		Sampling request = new Sampling(5, "foo", "sum");
		Set<ConstraintViolation<Sampling>> violations = BeanValidationHelper.VALIDATOR.validate(request);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
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