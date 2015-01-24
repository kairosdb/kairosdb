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

package org.kairosdb.core.datastore;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class Sampling extends Duration
{
	private DateTimeZone m_timeZone;

	public Sampling()
	{
		super();
	}

	public Sampling(int value, TimeUnit unit)
	{
		super(value, unit);
		this.m_timeZone = DateTimeZone.UTC;
	}

	public Sampling(int value, TimeUnit unit, DateTimeZone timeZone)
	{
		super(value, unit);
		this.m_timeZone = timeZone;
	}

	public DateTimeZone getTimeZone()
	{
		return m_timeZone;
	}

	/**
	 Computes the duration of the sampling (value * unit) starting at timestamp.

	 @param timestamp unix timestamp of the start time.
	 @return the duration of the sampling in millisecond.
	 */
	public long getSamplingDuration(long timestamp)
	{
		long sampling = (long) value;
		DateTime dt = new DateTime(timestamp, m_timeZone);
		switch (unit)
		{
			case YEARS:
				sampling = new org.joda.time.Duration(dt, dt.plusYears(value)).getMillis();
				break;
			case MONTHS:
				sampling = new org.joda.time.Duration(dt, dt.plusMonths(value)).getMillis();
				break;
			case WEEKS:
				sampling = new org.joda.time.Duration(dt, dt.plusWeeks(value)).getMillis();
				break;
			case DAYS:
				sampling = new org.joda.time.Duration(dt, dt.plusDays(value)).getMillis();
				break;
			case HOURS:
				sampling = new org.joda.time.Duration(dt, dt.plusHours(value)).getMillis();
				break;
			case MINUTES:
				sampling = new org.joda.time.Duration(dt, dt.plusMinutes(value)).getMillis();
				break;
			case SECONDS:
				sampling = new org.joda.time.Duration(dt, dt.plusSeconds(value)).getMillis();
				break;
			case MILLISECONDS:
				sampling = (long) value;
				break;
		}
		return sampling;
	}
}
