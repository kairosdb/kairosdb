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
import org.kairosdb.core.annotation.QueryProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.formatter.FormatterException;

import java.io.StringWriter;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 Groups data points by bin values. Data points are a range of values specified by bins.
 Bins array needs to be in ascending order.
 */
@QueryProcessor(
		name = "bin",
		description = "Groups data points by bins or buckets."
)
public class BinGroupBy implements GroupBy
{
    @QueryProperty(
            label = "Bin Values",
            description = "List of bin values. For example, if the list of bins is 10, 20, 30, then values less than 10 are placed in the first group, values between 10-19 into the second group, and so forth.",
            validations = {
            		@ValidationProperty(
            				expression = "value.length > 0",
							message = "The list can't be empty."
					),
					@ValidationProperty(
							expression = "value.every((n) => !isNaN(parseFloat(n)) && isFinite(n))",
							message = "The list must contain only numbers."
					)
			}
    )
	private double[] bins;

	public BinGroupBy()
	{
	}

	public BinGroupBy(double[] bins)
	{
		checkArgument(bins.length > 0);

		this.bins = bins;
	}

	@Override
	public int getGroupId(DataPoint dataPoint, Map<String, String> tags)
	{
		double dataValue = 0;
		if (dataPoint.isLong())
			dataValue = dataPoint.getLongValue();
		else if (dataPoint.isDouble())
			dataValue = dataPoint.getDoubleValue();
		else
			return -1;
		if (dataValue < bins[0])
			return 0;
		for (int i = 0; i < bins.length - 1; i++)
		{
			if (dataValue >= bins[i] && dataValue < bins[i + 1])
				return i + 1;
		}
		return bins.length;
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
					writer.key("name").value("bin");
					writer.key("bins").value(bins);

					writer.key("group").object();
					writer.key("bin_number").value(id);
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

	public void setBins(double[] bins)
	{
		this.bins = bins;
	}
}