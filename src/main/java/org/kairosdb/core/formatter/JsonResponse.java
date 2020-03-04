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
import org.json.JSONObject;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

public class JsonResponse
{
	private Writer m_writer;
	private JSONWriter m_jsonWriter;

	public JsonResponse(Writer writer)
	{
		m_writer = writer;
		m_jsonWriter = new JSONWriter(writer);
	}

	public void begin(String originalQuery) throws FormatterException
	{
		try
		{
			m_jsonWriter.object();
			if (originalQuery != null)
				m_jsonWriter.key("original_query").value(new JSONObject(originalQuery));
			m_jsonWriter.key("queries").array();
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
	}

	/**
	 * Formats the query results
	 *
	 * @param queryResults results of the query
	 * @param excludeTags if true do not include tag information
	 * @param sampleSize   Passing a sample size of -1 will cause the attribute to not show up
	 * @throws FormatterException
	 */
	public void formatQuery(List<DataPointGroup> queryResults, boolean excludeTags, int sampleSize) throws FormatterException
	{
		try
		{
			m_jsonWriter.object();

			if (sampleSize != -1)
				m_jsonWriter.key("sample_size").value(sampleSize);

			m_jsonWriter.key("results").array();

			//This loop must call close on each group at the end.
			for (DataPointGroup group : queryResults)
			{
				final String metric = group.getName();

				m_jsonWriter.object();
				m_jsonWriter.key("name").value(metric);

				if (!group.getGroupByResult().isEmpty())
				{
					m_jsonWriter.key("group_by");
					m_jsonWriter.array();
					boolean first = true;
					for (GroupByResult groupByResult : group.getGroupByResult())
					{
						if (!first)
							m_writer.write(",");
						m_writer.write(groupByResult.toJson());
						first = false;
					}
					m_jsonWriter.endArray();
				}

				if (!excludeTags)
				{
					//todo move this to after the values are obtained so we can filter unused tags
					m_jsonWriter.key("tags").object();

					for (String tagName : group.getTagNames())
					{
						m_jsonWriter.key(tagName);
						m_jsonWriter.value(group.getTagValues(tagName));
					}
					m_jsonWriter.endObject();
				}

				m_jsonWriter.key("values").array();
				while (group.hasNext())
				{
					DataPoint dataPoint = group.next();

					m_jsonWriter.array().value(dataPoint.getTimestamp());
					dataPoint.writeValueToJson(m_jsonWriter);

					/*if (dataPoint.isInteger())
					{
						m_jsonWriter.value(dataPoint.getLongValue());
					}
					else
					{
						final double value = dataPoint.getDoubleValue();
						if (value != value || Double.isInfinite(value))
						{
							throw new IllegalStateException("NaN or Infinity:" + value + " data point=" + dataPoint);
						}
						m_jsonWriter.value(value);
					}*/
					m_jsonWriter.endArray();
				}
				m_jsonWriter.endArray();
				m_jsonWriter.endObject();

				//Don't close the group the caller will do that.
			}

			m_jsonWriter.endArray().endObject();
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

	public void end() throws FormatterException
	{
		try
		{
			m_jsonWriter.endArray();
			m_jsonWriter.endObject();
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
	}
}
