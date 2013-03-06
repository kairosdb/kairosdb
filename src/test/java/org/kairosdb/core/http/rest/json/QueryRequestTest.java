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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;

public class QueryRequestTest
{
	@Test
	/**
	 * Verify that the TimeUnitRequired validator is called for the Sampling unit.
	 */
	public void testValidRequest()
	{
		List<Metric> metricList = new ArrayList<Metric>();

		Sampling sampling = new Sampling(1, "minutes", "sum");
		metricList.add(new Metric("name", "avg", sampling, true, null, null));

		QueryRequest queryRequest = new QueryRequest(null, new RelativeTime(5, "hours"), null,
				new RelativeTime(6, "years"), 0, metricList);
		Set<ConstraintViolation<QueryRequest>> violations = BeanValidationHelper.VALIDATOR.validate(queryRequest);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(0));
	}

	@Test
	/**
	 * Verify that the TimeUnitRequired validator is called for the Sampling unit.
	 */
	public void testSamplingValidationUnitInvalid()
	{
		List<Metric> metricList = new ArrayList<Metric>();

		Sampling sampling = new Sampling(1, "invalidUnit", "sum");
		metricList.add(new Metric("name", "avg", sampling, true, null, null));

		QueryRequest queryRequest = new QueryRequest("1", null, null, null, 0, metricList);
		Set<ConstraintViolation<QueryRequest>> violations = BeanValidationHelper.VALIDATOR.validate(queryRequest);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("metrics[0].sampling.unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}

	@Test
	/**
	 * Verify that the TimeUnitRequired validator is called for the RelativeTime unit.
	 */
	public void testRelativeTimeValidationUnitInvalid()
	{
		List<Metric> metricList = new ArrayList<Metric>();

		Sampling sampling = new Sampling(1, "minutes", "sum");
		metricList.add(new Metric("name", "avg", sampling, true, null, null));

		QueryRequest queryRequest = new QueryRequest(null, new RelativeTime(5, "invalidUnit"), null,
				new RelativeTime(6, "invalidUnit"), 0, metricList);
		Set<ConstraintViolation<QueryRequest>> violations = BeanValidationHelper.VALIDATOR.validate(queryRequest);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(2));
		assertThat(violationMessages, hasItem("startRelative.unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
		assertThat(violationMessages, hasItem("endRelative.unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}
}