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

import org.kairosdb.core.exception.TsdbException;
import org.kairosdb.testing.BeanValidationHelper;
import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class RelativeTimeTest
{
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
	private long timeRelativeTo;

	@Before
	public void setup() throws ParseException
	{
		timeRelativeTo = dateFormat.parse("2013-JAN-18 4:55:12").getTime();
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_getTimeRelativeTo_invalidUnit() throws TsdbException
	{
		RelativeTime time = new RelativeTime(5, "QUARTERS");

		time.getTimeRelativeTo(System.currentTimeMillis());
	}

	@Test
	public void test_getTimeRelativeTo_seconds() throws ParseException
	{
		RelativeTime time = new RelativeTime(5, "SECONDS");

		assertThat(time.getTimeRelativeTo(timeRelativeTo), equalTo(dateFormat.parse("2013-Jan-18 4:55:07").getTime()));
	}

	@Test
	public void test_getTimeRelativeTo_minutes() throws ParseException
	{
		RelativeTime time = new RelativeTime(5, "MINUTES");

		assertThat(time.getTimeRelativeTo(timeRelativeTo), equalTo(dateFormat.parse("2013-Jan-18 4:50:12").getTime()));
	}

	@Test
	public void test_getTimeRelativeTo_hours() throws ParseException
	{
		RelativeTime time = new RelativeTime(3, "HOURS");

		assertThat(time.getTimeRelativeTo(timeRelativeTo), equalTo(dateFormat.parse("2013-Jan-18 1:55:12").getTime()));
	}

	@Test
	public void test_getTimeRelativeTo_days() throws ParseException
	{
		RelativeTime time = new RelativeTime(3, "DAYS");

		assertThat(time.getTimeRelativeTo(timeRelativeTo), equalTo(dateFormat.parse("2013-Jan-15 4:55:12").getTime()));
	}

	@Test
	public void test_getTimeRelativeTo_weeks() throws ParseException
	{
		RelativeTime time = new RelativeTime(3, "WEEKS");

		assertThat(time.getTimeRelativeTo(timeRelativeTo), equalTo(dateFormat.parse("2012-Dec-28 4:55:12").getTime()));
	}

	@Test
	public void test_getTimeRelativeTo_Months() throws ParseException
	{
		RelativeTime time = new RelativeTime(2, "MONTHS");

		assertThat(time.getTimeRelativeTo(timeRelativeTo), equalTo(dateFormat.parse("2012-Nov-18 4:55:12").getTime()));
	}

	@Test
	public void test_getTimeRelativeTo_years() throws ParseException
	{
		RelativeTime time = new RelativeTime(3, "YEARS");

		assertThat(time.getTimeRelativeTo(timeRelativeTo), equalTo(dateFormat.parse("2010-JAN-18 4:55:12").getTime()));
	}

	@Test
	public void testValueZeroInvalid()
	{
		RelativeTime time = new RelativeTime(0, "days");
		Set<ConstraintViolation<RelativeTime>> violations = BeanValidationHelper.VALIDATOR.validate(time);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("value must be greater than or equal to 1"));
	}

	@Test
	public void testUnitNullInvalid()
	{
		RelativeTime time = new RelativeTime(1, null);
		Set<ConstraintViolation<RelativeTime>> violations = BeanValidationHelper.VALIDATOR.validate(time);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("unit must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}

	@Test
	public void testUnitEmptyInvalid()
	{
		RelativeTime time = new RelativeTime(1, "");
		Set<ConstraintViolation<RelativeTime>> violations = BeanValidationHelper.VALIDATOR.validate(time);
		List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

		assertThat(violationMessages.size(), equalTo(1));
		assertThat(violationMessages.get(0), equalTo("unit must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"));
	}
}