/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.core.datastore;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.util.Util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class SamplingTest
{
	@Test
	public void test_getUnitDuration_year_no_leap() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.YEARS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 12, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(365 * 24 * 60 * 60 * 1000L));
	}

	@Test
	public void test_getUnitDuration_year_over_leap() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.YEARS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2012, 2, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(366 * 24 * 60 * 60 * 1000L));
	}

	@Test
	public void test_getUnitDuration_month_january() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.MONTHS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 1, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(31 * 24 * 60 * 60 * 1000L));
	}

	@Test
	public void test_getUnitDuration_month_february() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.MONTHS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 2, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(28 * 24 * 60 * 60 * 1000L));
	}

	@Test
	public void test_getUnitDuration_week() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.WEEKS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 12, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(7 * 24 * 60 * 60 * 1000L));
	}

	@Test
	public void test_getUnitDuration_day() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.DAYS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 12, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(24 * 60 * 60 * 1000L));
	}

	@Test
	public void test_getUnitDuration_hour() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.HOURS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 12, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(60 * 60 * 1000L));
	}

	@Test
	public void test_getUnitDuration_minute() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.MINUTES);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 12, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(1000 * 60L));
	}

	@Test
	public void test_getUnitDuration_seconds() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.SECONDS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 12, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(1000L));
	}

	@Test
	public void test_getUnitDuration_milliseconds() throws Exception
	{
		Sampling sampling = new Sampling(1, TimeUnit.MILLISECONDS);
		DateTimeZone timezone = DateTimeZone.forID("Europe/Brussels");
		DateTime dt = new DateTime(2014, 12, 18, 1, 2, 3, 4, timezone);

		assertThat(Util.getSamplingDuration(dt.getMillis(), sampling, timezone), is(1L));
	}
}
