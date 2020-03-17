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
package org.kairosdb.core.formatter;


import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JsonFormatter implements DataFormatter
{
	@Override
	public void format(Writer writer, Iterable<String> iterable) throws FormatterException
	{
		requireNonNull(writer);
		requireNonNull(iterable);

		try
		{
			JSONWriter jsonWriter = new JSONWriter(writer);
			jsonWriter.object().key("results").array();
			for (String string : iterable)
			{
				jsonWriter.value(string);
			}
			jsonWriter.endArray().endObject();
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
	}

	@Override
	public void format(Writer writer, List<List<DataPointGroup>> data) throws FormatterException
	{

		requireNonNull(writer);
		requireNonNull(data);
		try
		{
			JSONWriter jsonWriter = new JSONWriter(writer);

			jsonWriter.object().key("queries").array();

			for (List<DataPointGroup> groups : data)
			{
				jsonWriter.object().key("results").array();

				for (DataPointGroup group : groups)
				{
					final String metric = group.getName();

					jsonWriter.object();
					jsonWriter.key("name").value(metric);

					if (!group.getGroupByResult().isEmpty())
					{
						jsonWriter.key("group_by");
						jsonWriter.array();
						boolean first = true;
						for (GroupByResult groupByResult : group.getGroupByResult())
						{
							if (!first)
								writer.write(",");
							writer.write(groupByResult.toJson());
							first = false;
						}
						jsonWriter.endArray();
					}

					jsonWriter.key("tags").object();

					for (String tagName : group.getTagNames())
					{
						jsonWriter.key(tagName);
						jsonWriter.value(group.getTagValues(tagName));
					}
					jsonWriter.endObject();

					jsonWriter.key("values").array();
					while (group.hasNext())
					{
						DataPoint dataPoint = group.next();

						jsonWriter.array().value(dataPoint.getTimestamp());

						dataPoint.writeValueToJson(jsonWriter);

						/*
						if (dataPoint.isInteger())
						{
							jsonWriter.value(dataPoint.getLongValue());
						}
						else
						{
							final double value = dataPoint.getDoubleValue();
							if (value != value || Double.isInfinite(value))
							{
								throw new IllegalStateException("NaN or Infinity:" + value + " data point=" + dataPoint);
							}
							jsonWriter.value(value);
						}*/

						//jsonWriter.value(dataPoint.getApiDataType());
						jsonWriter.endArray();
					}
					jsonWriter.endArray();
					jsonWriter.endObject();

					group.close();
				}

				jsonWriter.endArray().endObject();
			}

			jsonWriter.endArray().endObject();
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
		catch (IOException e)
		{
			throw new FormatterException(e);
		}
	}
}