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

package org.kairosdb.core.http.rest.json;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.NameValidator;
import org.kairosdb.util.ValidationException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Originally used Jackson to parse, but this approach failed for a very large JSON because
 * everything was in memory and we would run out of memory. This parser adds metrics as it walks
 * through the stream.
 */
public class JsonMetricParser
{
	private KairosDatastore datastore;
	private Reader inputStream;

	public JsonMetricParser(KairosDatastore datastore, Reader stream)
	{
		this.datastore = checkNotNull(datastore);
		this.inputStream = checkNotNull(stream);
	}

	public void parse() throws IOException, ValidationException, DatastoreException
	{
		JsonReader reader = new JsonReader(inputStream);

		try
		{
			int metricCount = 0;

			if (reader.peek().equals(JsonToken.BEGIN_ARRAY))
			{
				try
				{
					reader.beginArray();
				}
				catch(EOFException e)
				{
					throw new ValidationException("Invalid json. No content due to end of input.");
				}
				while(reader.hasNext())
				{
					parseMetric(reader, metricCount);
					metricCount++;
				}

				reader.endArray();
			}
			else if (reader.peek().equals(JsonToken.BEGIN_OBJECT))
			{
				parseMetric(reader, 0);
			}
			else
				throw new ValidationException("Invalid start of json.");

		}
		catch(EOFException e)
		{
			throw new ValidationException("Invalid json. No content due to end of input.");
		}
		finally
		{
			reader.close();
		}
	}

	private void parseMetric(JsonReader reader, int count) throws IOException, ValidationException, DatastoreException
	{
		reader.beginObject();
		String name = null;
		long timestamp = 0;
		String value = null;
		Map<String, String> tags = new HashMap<String, String>();
		List<DataPoint> dataPoints = new ArrayList<DataPoint>();
		while (reader.hasNext())
		{
			String token = reader.nextName();
			if (token.equals("name"))
			{
				name = reader.nextString();
			}
			else if (token.equals("timestamp"))
			{
				timestamp = reader.nextLong();
				NameValidator.validateMin("timestamp", timestamp, 1);
			}
			//This is here as this code is now handling the import files.
			//We had a pre 0.9.2 import file that contained time instead of timestamp
			else if (token.equals("time"))
			{
				timestamp = reader.nextLong();
				NameValidator.validateMin("time", timestamp, 1);
			}
			else if (token.equals("value"))
			{
				value = reader.nextString();
				NameValidator.validateNotNullOrEmpty("value", value);
			}
			else if (token.equals("datapoints"))
			{
				dataPoints = parseDataPoints(reader);
			}
			else if (token.equals("tags"))
			{
				tags = parseTags(reader,count);
			}
		}
		reader.endObject();

		if (timestamp > 0 && (value == null || value.isEmpty()))
			throw new ValidationException("metric[" + count + "].value cannot be null or empty.");
		if (value != null && timestamp < 1)
			throw new ValidationException("metric[" + count + "].timestamp must be greater than 0.");
		if (tags.size() < 1)
			throw new ValidationException("metric[" + count + "].tags cannot be null or empty.");


		if (timestamp > 0 && value != null)
		{
			if (value.contains("."))
				dataPoints.add(new DataPoint(timestamp, Double.parseDouble(value)));
			else
				dataPoints.add(new DataPoint(timestamp, Long.parseLong(value)));
		}

		NameValidator.validateNotNullOrEmpty("metric[" + count + "].name", name);
		NameValidator.validateCharacterSet("metric[" + count + "].name", name);

		datastore.putDataPoints(new DataPointSet(name, tags, dataPoints));
	}

	private Map<String, String> parseTags(JsonReader reader, int metricCount) throws IOException, ValidationException
	{
		Map<String, String> tags = new HashMap<String, String>();
		reader.beginObject();
		int tagCount = 0;
		while(reader.hasNext())
		{
			String tagName = reader.nextName();
			NameValidator.validateNotNullOrEmpty(String.format("metric[%d].tag[%d].name", metricCount, tagCount), tagName);
			NameValidator.validateCharacterSet(String.format("metric[%d].tag[%d].name", metricCount, tagCount), tagName);

			String value = reader.nextString();
			NameValidator.validateNotNullOrEmpty(String.format("metric[%d].tag[%d].value", metricCount, tagCount), value);
			NameValidator.validateCharacterSet(String.format("metric[%d].tag[%d].value", metricCount, tagCount), value);

			tags.put(tagName, value);
			tagCount++;
		}
		reader.endObject();
		return tags;
	}

	private List<DataPoint> parseDataPoints(JsonReader reader) throws IOException, ValidationException
	{
		List<DataPoint> dataPoints = new ArrayList<DataPoint>();
		reader.beginArray();
		while(reader.hasNext())
		{
			reader.beginArray();
			long timestamp = reader.nextLong();
			NameValidator.validateMin("datapoints.timestamp", timestamp, 1);

			String value = reader.nextString();
			NameValidator.validateNotNullOrEmpty("value", value);

			if (value.contains("."))
				dataPoints.add(new DataPoint(timestamp, Double.parseDouble(value)));
			else
					dataPoints.add(new DataPoint(timestamp, Long.parseLong(value)));
			reader.endArray();
		}
		reader.endArray();

		return dataPoints;
	}
}