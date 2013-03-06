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

public class NewMetricRequestTest
{
//	@Test
//	public void testNullNameInvalid()
//	{
//		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(new NewMetricRequest(null, "value", 5, null));
//		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);
//
//		assertThat(violationMessages.size(), equalTo(1));
//		assertThat(violationMessages.get(0), equalTo("name may not be null"));
//
//	}
//
//	@Test
//	public void testEmptyNameInvalid()
//	{
//		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(new NewMetricRequest("", "value", 5, null));
//		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);
//
//		assertThat(violationMessages.size(), equalTo(1));
//		assertThat(violationMessages.get(0), equalTo("name may not be empty"));
//
//	}
//
//	@Test
//	public void testNullValueInvalid()
//	{
//		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(new NewMetricRequest("the name", null, 5, null));
//		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);
//
//		assertThat(violationMessages.size(), equalTo(1));
//		assertThat(violationMessages.get(0), equalTo("value may not be null"));
//
//	}
//
//	@Test
//	public void testEmptyValueInvalid()
//	{
//		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(new NewMetricRequest("the name", "", 5, null));
//		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);
//
//		assertThat(violationMessages.size(), equalTo(1));
//		assertThat(violationMessages.get(0), equalTo("value may not be empty"));
//
//	}
//
//	@Test
//	public void testTimestampZeroInvalid()
//	{
//		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(new NewMetricRequest("the name", "value", 0, null));
//		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);
//
//		assertThat(violationMessages.size(), equalTo(1));
//		assertThat(violationMessages.get(0), equalTo("timestamp must be greater than or equal to 1"));
//
//	}
}