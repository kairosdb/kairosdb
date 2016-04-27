/*
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

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public enum TimeUnit
{
	MILLISECONDS,
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

