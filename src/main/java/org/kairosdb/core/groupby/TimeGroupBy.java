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

package org.kairosdb.core.groupby;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.GroupByName;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.formatter.FormatterException;

import java.io.StringWriter;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@GroupByName(name = "time", description = "Groups data points in time ranges.")
public class TimeGroupBy implements GroupBy
{
	private Duration rangeSize;
	private int groupCount;
	private long startDate;

	public TimeGroupBy()
	{
	}

	public TimeGroupBy(Duration rangeSize, int groupCount)
	{
		checkArgument(groupCount > 0);

		this.rangeSize = checkNotNull(rangeSize);
		this.groupCount = groupCount;
	}

	@Override
	public int getGroupId(DataPoint dataPoint, Map<String, String> tags)
	{
	 	return (int) (((dataPoint.getTimestamp() - startDate) / convertGroupSizeToMillis() ) % groupCount);
	}

	@SuppressWarnings("NumericOverflow")
	private long convertGroupSizeToMillis()
	{
		long milliseconds = rangeSize.getValue();
		switch(rangeSize.getUnit())
		{
			case MONTHS: milliseconds *= 30L * 7L * 24L * 60L * 60L * 1000L;
				break;
			case YEARS: milliseconds *= 52;
			case WEEKS: milliseconds *= 7;
			case DAYS: milliseconds *= 24;
			case HOURS: milliseconds *= 60;
			case MINUTES: milliseconds *= 60;
			case SECONDS: milliseconds *= 1000;
		}

		return milliseconds;
	}

	public void setRangeSize(Duration rangeSize)
	{
		this.rangeSize = rangeSize;
	}

	public void setGroupCount(int groupCount)
	{
		this.groupCount = groupCount;
	}

	@Override
	public GroupByResult getGroupByResult(final int id)
	{
		return new GroupByResult()
		{
			@Override
			public String toJson() throws FormatterException
			{
				StringWriter stringWriter = new StringWriter();
				try
				{
					JSONWriter writer = new JSONWriter(stringWriter);

					writer.object();
					writer.key("name").value("time");
					writer.key("range_size").object();
					writer.key("value").value(rangeSize.getValue());
					writer.key("unit").value(rangeSize.getUnit().toString());
					writer.endObject();
					writer.key("group_count").value(groupCount);
					writer.key("group").object();
					writer.key("group_number").value(id);
					writer.endObject();
					writer.endObject();
				}
				catch (JSONException e)
				{
					throw new FormatterException(e);
				}

				return stringWriter.toString();
			}
		};

	}

	@Override
	public void setStartDate(long startDate)
	{
		this.startDate = startDate;
	}
}