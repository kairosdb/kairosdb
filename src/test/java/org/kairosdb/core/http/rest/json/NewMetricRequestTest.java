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
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class NewMetricRequestTest
{
	@Test
	public void testNullNameInvalid()
	{
		NewMetricRequest request = new NewMetricRequest(null, new HashMap<String, String>());
		request.addDataPoint(new DataPointRequest(5, "value"));
		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("name may not be null"));

	}

	@Test
	public void testEmptyNameInvalid()
	{
		NewMetricRequest request = new NewMetricRequest("", new HashMap<String, String>());
		request.addDataPoint(new DataPointRequest(5, "value"));
		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("name may not be empty"));

	}

	@Test
	public void testNullValueInvalid()
	{
		NewMetricRequest request = new NewMetricRequest("metric1", new HashMap<String, String>());
		request.addDataPoint(new DataPointRequest(5, null));

		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("datapoints[0].value may not be null"));

	}

	@Test
	public void testEmptyValueInvalid()
	{
		NewMetricRequest request = new NewMetricRequest("metric1", new HashMap<String, String>());
		request.addDataPoint(new DataPointRequest(5, ""));

		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("datapoints[0].value may not be empty"));

	}

	@Test
	public void testTimestampZeroInvalid()
	{
		NewMetricRequest request = new NewMetricRequest("metric1", new HashMap<String, String>());
		request.addDataPoint(new DataPointRequest(0, "value"));

		Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("datapoints[0].timestamp must be greater than or equal to 1"));

	}
}