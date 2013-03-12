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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.validation.JsonValidator;
import org.kairosdb.core.http.rest.validation.ValidationException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
	private Datastore datastore;
	private InputStream inputStream;

	public JsonMetricParser(Datastore datastore, InputStream stream)
	{
		this.datastore = checkNotNull(datastore);
		this.inputStream = checkNotNull(stream);
	}

	public void parse() throws IOException, ValidationException, DatastoreException
	{
		JsonReader reader = new JsonReader(new InputStreamReader(inputStream));

		try
		{
			int metricCount = 0;
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
		finally {
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
				JsonValidator.validateMin("timestamp", timestamp, 1);
			}
			else if (token.equals("value"))
			{
				value = reader.nextString();
				JsonValidator.validateNotNullOrEmpty("value", value);
			}
			else if (token.equals("datapoints"))
			{
				dataPoints = parseDataPoints(reader);
			}
			else if (token.equals("tags"))
			{
				tags = parseTags(reader);
			}
		}
		reader.endObject();

		if (timestamp > 0 && (value == null || value.isEmpty()))
			throw new ValidationException("metric[" + count + "].value cannot be null or empty.");
		if (value != null && timestamp < 1)
			throw new ValidationException("metric[" + count + "].timestamp must be greater than 0.");


		if (timestamp > 0 && value != null)
		{
			if (value.contains("."))
				dataPoints.add(new DataPoint(timestamp, Double.parseDouble(value)));
			else
				dataPoints.add(new DataPoint(timestamp, Long.parseLong(value)));
		}

		JsonValidator.validateNotNullOrEmpty("metric[" + count + "].name", name);

		datastore.putDataPoints(new DataPointSet(name, tags, dataPoints));
	}

	private Map<String, String> parseTags(JsonReader reader) throws IOException
	{
		Map<String, String> tags = new HashMap<String, String>();
		reader.beginObject();
		while(reader.hasNext())
		{
			String tagName = reader.nextName();
			tags.put(tagName, reader.nextString());
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
			JsonValidator.validateMin("datapoints.timestamp", timestamp, 1);

			String value = reader.nextString();
			JsonValidator.validateNotNullOrEmpty("value", value);

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