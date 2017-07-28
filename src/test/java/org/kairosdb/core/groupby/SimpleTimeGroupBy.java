/*
 * Copyright 2016 KairosDB Authors
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
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.formatter.FormatterException;

import java.io.StringWriter;
import java.util.Map;

@QueryProcessor(
		name = "simpleTime",
		description = "Groups data points by time."
)
public class SimpleTimeGroupBy implements GroupBy
{
	private int rangeSize;

	public SimpleTimeGroupBy()
	{
		rangeSize = 2;
	}

	public SimpleTimeGroupBy(int rangeSize)
	{
		this.rangeSize = rangeSize;
	}

	@Override
	public int getGroupId(DataPoint dataPoint, Map<String, String> tags)
	{
		return (int) (dataPoint.getTimestamp() / rangeSize);
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
					writer.key("name").value("simpleTime");
					writer.key("target_size").value(rangeSize);

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
	}

	public void setRangeSize(int groupSize)
	{
		this.rangeSize = groupSize;
	}
}