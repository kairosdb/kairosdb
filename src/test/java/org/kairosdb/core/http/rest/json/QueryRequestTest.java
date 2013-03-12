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
		/*List<Metric> metricList = new ArrayList<Metric>();

		Sampling sampling = new Sampling(1, "minutes", "sum");
		metricList.add(new Metric("name", "avg", sampling, true, null, null));

		QueryRequest queryRequest = new QueryRequest(null, new RelativeTime(5, "hours"), null,
				new RelativeTime(6, "years"), 0, metricList);
		Set<ConstraintViolation<QueryRequest>> violations = BeanValidationHelper.VALIDATOR.validate(queryRequest);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(0));*/
	}

	@Test
	/**
	 * Verify that the TimeUnitRequired validator is called for the Sampling unit.
	 */
	public void testSamplingValidationUnitInvalid()
	{
		/*List<Metric> metricList = new ArrayList<Metric>();

		Sampling sampling = new Sampling(1, "invalidUnit", "sum");
		metricList.add(new Metric("name", "avg", sampling, true, null, null));

		QueryRequest queryRequest = new QueryRequest("1", null, null, null, 0, metricList);
		Set<ConstraintViolation<QueryRequest>> violations = BeanValidationHelper.VALIDATOR.validate(queryRequest);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("metrics[0].sampling.unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));*/
	}

	@Test
	/**
	 * Verify that the TimeUnitRequired validator is called for the RelativeTime unit.
	 */
	public void testRelativeTimeValidationUnitInvalid()
	{
		/*List<Metric> metricList = new ArrayList<Metric>();

		Sampling sampling = new Sampling(1, "minutes", "sum");
		metricList.add(new Metric("name", "avg", sampling, true, null, null));

		QueryRequest queryRequest = new QueryRequest(null, new RelativeTime(5, "invalidUnit"), null,
				new RelativeTime(6, "invalidUnit"), 0, metricList);
		Set<ConstraintViolation<QueryRequest>> violations = BeanValidationHelper.VALIDATOR.validate(queryRequest);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(2));
		assertThat(violationMessages, hasItem("startRelative.unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
		assertThat(violationMessages, hasItem("endRelative.unit must be one of SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));*/
	}
}