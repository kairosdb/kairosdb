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

import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.http.rest.validation.TimeUnitRequired;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.constraints.Min;
import java.util.Calendar;
import java.util.TimeZone;

public class RelativeTime
{
	@Min(1)
	private int value;

	@TimeUnitRequired
	private String unit;

	private Calendar calendar;

	@JsonCreator
	public RelativeTime(@JsonProperty("value") int value, @JsonProperty("unit") String unit)
	{
		this.value = value;
		this.unit = unit;
		calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	}

	public int getValue()
	{
		return value;
	}

	public TimeUnit getUnit()
	{
		return TimeUnit.from(unit);
	}

	public long getTimeRelativeTo(long time)
	{
		int field = 0;
		if (getUnit() == TimeUnit.SECONDS )
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
		calendar.add(field, -value);

		return calendar.getTime().getTime();
	}
}