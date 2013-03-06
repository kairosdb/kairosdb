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

package org.kairosdb.core.datastore;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public enum TimeUnit
{
	SECONDS,
	MINUTES,
	HOURS,
	DAYS,
	WEEKS,
	MONTHS,
	YEARS;

	public static TimeUnit from(String value)
	{
		checkNotNullOrEmpty(value);
		for (TimeUnit unit : values())
		{
			if (unit.toString().equalsIgnoreCase(value))
			{
				return unit;
			}
		}

		throw new IllegalArgumentException("No enum constant for " + value);
	}

	public static boolean contains(String value)
	{
		for (TimeUnit unit : values())
		{
			if (unit.name().equalsIgnoreCase(value))
			{
				return true;
			}
		}

		return false;
	}

	public static String toValueNames()
	{
		StringBuilder builder = new StringBuilder();
		boolean firstTime = true;
		for (TimeUnit timeUnit : values())
		{
			if (!firstTime)
			{
				builder.append(',');
			}
			builder.append(timeUnit.name());
			firstTime = false;
		}
		return builder.toString();
	}
}

