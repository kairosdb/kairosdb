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
import org.kairosdb.util.Validator;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
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

	public ValidationErrors parse() throws IOException, DatastoreException
	{
		ValidationErrors validationErrors = new ValidationErrors();

		JsonReader reader = new JsonReader(inputStream);

		try
		{
			int metricCount = 0;

			if (reader.peek().equals(JsonToken.BEGIN_ARRAY))
			{
				try
				{
					reader.beginArray();

					while (reader.hasNext())
					{
						validationErrors.add(parseMetric(reader, metricCount));
						metricCount++;
					}
				}
				catch (EOFException e)
				{
					validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
				}

				reader.endArray();
			}
			else if (reader.peek().equals(JsonToken.BEGIN_OBJECT))
			{
				validationErrors.add(parseMetric(reader, 0));
			}
			else
				validationErrors.addErrorMessage("Invalid start of json.");

		}
		catch (EOFException e)
		{
			validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
		}
		finally
		{
			reader.close();
		}

		return validationErrors;
	}

	private ValidationErrors parseMetric(JsonReader reader, int count) throws IOException, DatastoreException
	{
		ValidationErrors validationErrors = new ValidationErrors();

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
			}
			//This is here as this code is now handling the import files.
			//We had a pre 0.9.2 import file that contained time instead of timestamp
			else if (token.equals("time"))
			{
				timestamp = reader.nextLong();
			}
			else if (token.equals("value"))
			{
				value = reader.nextString();
			}
			else if (token.equals("datapoints"))
			{
				dataPoints = parseDataPoints(reader, validationErrors);
			}
			else if (token.equals("tags"))
			{
				tags = parseTags(reader, validationErrors, count);
			}
		}
		reader.endObject();

		if (Validator.isNotNullOrEmpty(validationErrors, "metric[" + count + "].name", name))
			Validator.isValidateCharacterSet(validationErrors, "metric[" + count + "].name", name);

		if (timestamp > 0)
			Validator.isNotNullOrEmpty(validationErrors, "metric[" + count + "].value", value);
		if (value != null && !value.isEmpty())
			Validator.isGreaterThanOrEqualTo(validationErrors, "metric[" + count + "].timestamp", timestamp, 1);

		if (timestamp > 0 && value != null && !value.isEmpty())
		{
			dataPoints.add(createDataPoint(timestamp, value));
		}

		if (!validationErrors.hasErrors() &&
				Validator.isGreaterThanOrEqualTo(validationErrors, "metric[" + count + "].tags count", tags.size(), 1))
		{
			datastore.putDataPoints(new DataPointSet(name, tags, dataPoints));
		}

		return validationErrors;
	}

	private Map<String, String> parseTags(JsonReader reader, ValidationErrors validationErrors, int metricCount) throws IOException
	{
		Map<String, String> tags = new HashMap<String, String>();
		reader.beginObject();
		int tagCount = 0;
		boolean valid = true;
		while (reader.hasNext())
		{
			String tagName = reader.nextName();
			String value = reader.nextString();

			if (Validator.isNotNullOrEmpty(validationErrors, String.format("metric[%d].tag[%d].name", metricCount, tagCount), tagName) &&
					Validator.isValidateCharacterSet(validationErrors, String.format("metric[%d].tag[%d].name", metricCount, tagCount), tagName) &&
					Validator.isNotNullOrEmpty(validationErrors, String.format("metric[%d].tag[%d].value", metricCount, tagCount), value) &&
					Validator.isValidateCharacterSet(validationErrors, String.format("metric[%d].tag[%d].value", metricCount, tagCount), value))
			{
				tags.put(tagName, value);
				tagCount++;
			}
			else
				valid = false;
		}
		reader.endObject();

		return valid ? tags : null;
	}

	private List<DataPoint> parseDataPoints(JsonReader reader, ValidationErrors validationErrors) throws IOException
	{
		List<DataPoint> dataPoints = new ArrayList<DataPoint>();
		reader.beginArray();
		boolean valid = true;
		while (reader.hasNext())
		{
			reader.beginArray();
			long timestamp = reader.nextLong();

			String value = reader.nextString();

			if (Validator.isGreaterThanOrEqualTo(validationErrors, "datapoints.timestamp", timestamp, 1) &&
					Validator.isNotNullOrEmpty(validationErrors, "value", value))
				dataPoints.add(createDataPoint(timestamp, value));
			else
				valid = false;

			reader.endArray();
		}
		reader.endArray();

		return valid ? dataPoints : null;
	}

	private DataPoint createDataPoint(long timestamp, String value)
	{
		if (value.contains("."))
			return new DataPoint(timestamp, Double.parseDouble(value));
		else
			return new DataPoint(timestamp, Long.parseLong(value));
	}
}