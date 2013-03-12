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


import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class Duration
{
	@Min(1)
	protected int value;

	@NotNull
	protected TimeUnit unit;

	public Duration()
	{
	}

	public Duration(int value, TimeUnit unit)
	{
		this.value = value;
		this.unit = unit;
	}

	public int getValue()
	{
		return value;
	}

	public TimeUnit getUnit()
	{
		return unit;
	}

	@Override
	public String toString()
	{
		return "Duration{" +
				"value=" + value +
				", unit=" + unit +
				'}';
	}
}
