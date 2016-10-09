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
package org.kairosdb.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts a query response to a metric that could be pushed back into KairosDB.
 */
public class ResponseToMetricConverter
{
	Gson gson = new GsonBuilder().create();

	public static void main(String[] args) throws IOException
	{
		ResponseToMetricConverter converter = new ResponseToMetricConverter();
		File outFile = new File(args[1]);
		converter.convert(new FileInputStream(args[0]), new FileOutputStream(outFile));
	}

	public void convert(InputStream inputStream, OutputStream outStream) throws IOException
	{

		try (JsonReader reader = new JsonReader(new InputStreamReader(inputStream));
		     JsonWriter writer = new JsonWriter(new OutputStreamWriter(outStream)))
		{
			writer.beginArray();

			// Queries array
			reader.beginObject();
			while (reader.hasNext())
			{
				String token = reader.nextName();
				if (token.equals("queries"))
				{
					reader.beginArray();

					while (reader.hasNext())
					{
						reader.beginObject();
						token = reader.nextName();
						if (token.equals("results"))
						{
							parseResults(reader, writer);
						}
						reader.endObject();
					}

					reader.endArray();
				}
			}
			reader.endObject();

			writer.endArray();
		}
		catch (RuntimeException e)
		{
			e.printStackTrace();
		}
	}

	private void parseResults(JsonReader reader, JsonWriter writer) throws IOException
	{
		reader.beginArray();
		while (reader.hasNext())
		{
			MetricFrom metricFrom = gson.fromJson(reader, MetricFrom.class);
			MetricTo metricTo = new MetricTo(metricFrom);
			gson.toJson(metricTo, MetricTo.class, writer);
		}

		reader.endArray();
	}

	private static class MetricFrom
	{
		private String name;

		private Map<String, String[]> tags;

		private long[][] values;

		private GroupBy[] groupBy;
	}

	private class GroupBy
	{
		private String name;

		private String[] tags;

		private Map<String, String> group;
	}

	private static class MetricTo
	{
		private String name;
		private Map<String, String> tags = new HashMap<String, String>();
		private long[][] datapoints;

		private MetricTo(MetricFrom from)
		{
			this.name = from.name;
			this.datapoints = Arrays.copyOf(from.values, from.values.length);

			for (Map.Entry<String, String[]> entry : from.tags.entrySet())
			{
				tags.put(entry.getKey(), entry.getValue()[0]);
			}
		}
	}
}