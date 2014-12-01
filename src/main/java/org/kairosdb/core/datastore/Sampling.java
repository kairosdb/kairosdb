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
    private DateTimeZone timeZone;

    public Sampling()
	{
		super();
	}

	public Sampling(int value, TimeUnit unit)
	{
		super(value, unit);
        this.timeZone = DateTimeZone.UTC;
	}

    public Sampling(int value, TimeUnit unit, DateTimeZone timeZone) {
        super(value, unit);
        this.timeZone = timeZone;
    }

	/**
	 Works for any time unit except month and years.  Months and years are special cased in
	 the RangeAggregator
     // TODO get Sampling for a given time (eg. sampling is different in January (31d) or February (28-29d)
	 @return the number of milliseconds in the sampling range
	 */
	public long getSampling()
	{
        long sampling = value;
		switch (unit)
		{
            case YEARS:
                sampling =  value * new DateTime(timeZone).year().getDurationField().getUnitMillis();
			case WEEKS:
                sampling =  value * new DateTime(timeZone).weekOfWeekyear().getDurationField().getUnitMillis();
			case DAYS:
                sampling =  value * new DateTime(timeZone).dayOfMonth().getDurationField().getUnitMillis();
			case HOURS:
                sampling =  value * new DateTime(timeZone).hourOfDay().getDurationField().getUnitMillis();
			case MINUTES:
                sampling =  value * new DateTime(timeZone).minuteOfHour().getDurationField().getUnitMillis();
			case SECONDS:
                sampling =  value * new DateTime(timeZone).secondOfMinute().getDurationField().getUnitMillis();
			case MILLISECONDS:
                sampling =  value * new DateTime(timeZone).millisOfDay().getDurationField().getUnitMillis();
		}
        return sampling;
	}

    public DateTimeZone getTimeZone() {
        return timeZone;
    }
}
