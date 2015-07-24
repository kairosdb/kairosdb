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

import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.TimeUnit;

import java.util.Calendar;
import java.util.TimeZone;

public class RelativeTime extends Duration
{
	private Calendar calendar;

	private void initialize()
	{
		calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	}

	public RelativeTime()
	{
		initialize();
	}

	public RelativeTime(int value, String unit)
	{
		super(value, TimeUnit.from(unit));
		initialize();
	}

	public long getTimeRelativeTo(long time)
	{
		int valueToUse = -(int)value;
		int field = 0;
		if (getUnit() == TimeUnit.MILLISECONDS)
		{
			field = Calendar.MILLISECOND;
			//we do our own adjustment as calendar cannot handle a long.
			time -= value;
			valueToUse = 0;
		}
		else if (getUnit() == TimeUnit.SECONDS )
			field = Calendar.SECOND;
		else if (getUnit() == TimeUnit.MINUTES)
			field = Calendar.MINUTE;
		else if (getUnit() == TimeUnit.HOURS)
			field = Calendar.HOUR;
		else if (getUnit() == TimeUnit.DAYS)
			field = Calendar.DATE;
		else if (getUnit() == TimeUnit.WEEKS)
			field = Calendar.WEEK_OF_MONTH;
		else if (getUnit() == TimeUnit.MONTHS)
			field = Calendar.MONTH;
		else if (getUnit() == TimeUnit.YEARS)
			field = Calendar.YEAR;

		calendar.setTimeInMillis(time);
		calendar.add(field, valueToUse);

		return calendar.getTime().getTime();
	}
}