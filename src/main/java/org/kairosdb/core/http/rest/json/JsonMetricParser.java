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

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.json.JSONTokener;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.Validator;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
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
	private Gson gson;

	public int getDataPointCount()
	{
		return dataPointCount;
	}

	public int getIngestTime()
	{
		return ingestTime;
	}

	private int dataPointCount;
	private int ingestTime;

	public JsonMetricParser(KairosDatastore datastore, Reader stream)
	{
		this.datastore = checkNotNull(datastore);
		this.inputStream = checkNotNull(stream);
		GsonBuilder builder = new GsonBuilder();
		gson = builder.create();
	}

	public ValidationErrors parse() throws IOException, DatastoreException
	{
		long start = System.currentTimeMillis();
		ValidationErrors validationErrors = new ValidationErrors();

		JsonReader reader = new JsonReader(inputStream);
		JsonParser parser = new JsonParser();

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
						NewMetric metric = parseMetric(reader);
						//JsonObject metric = (JsonObject)parser.parse(reader);
						validateAndAddDataPoints(metric, validationErrors, metricCount);
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
				NewMetric metric = parseMetric(reader);
				//JsonObject metric = (JsonObject)parser.parse(reader);
				validateAndAddDataPoints(metric, validationErrors, 0);
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

		ingestTime = (int)(System.currentTimeMillis() - start);

		return validationErrors;
	}

	private NewMetric parseMetric(JsonReader reader)
	{
		NewMetric metric;
		try
		{
			metric = gson.fromJson(reader, NewMetric.class);
		}
		catch (IllegalArgumentException e)
		{
			// Happens when parsing data points where one of the pair is missing (timestamp or value)
			throw new JsonSyntaxException("Invalid JSON");
		}
		return metric;
	}

	private Map<String, String> getAsMap(JsonObject object)
	{
		Map<String, String> ret = new HashMap<String, String>();

		for (Map.Entry<String, JsonElement> entry : object.entrySet())
		{
			ret.put(entry.getKey(), entry.getValue().getAsString());
		}

		return (ret);
	}

	private boolean validateAndAddDataPoints(JsonObject metric, ValidationErrors errors, int count) throws DatastoreException
	{

		ValidationErrors validationErrors = new ValidationErrors();

		String context = "metric[" + count + "]";

		if (!validationErrors.hasErrors())
		{
			Map<String, String> tags = getAsMap(metric.get("tags").getAsJsonObject());
			DataPointSet dataPointSet = new DataPointSet(metric.get("name").getAsString(), tags, Collections.<DataPoint>emptyList());

			long time = 0L;
			if (metric.has("time"))
				time = metric.get("time").getAsLong();
			else if (metric.has("timestamp"))
				time = metric.get("timestamp").getAsLong();

			if (time > 0 && metric.has("value") && !metric.get("value").getAsString().isEmpty())
			{
				dataPointSet.addDataPoint(new DataPoint(time, metric.get("value").getAsString()));
			}

			if (metric.has("datapoints") && metric.get("datapoints").getAsJsonArray().size() > 0)
			{
				int dataPointCount = 0;
				String dataPointContext = context + ".datapoints[" + dataPointCount + "]";
				JsonArray datapoints = metric.get("datapoints").getAsJsonArray();

				for (int I = 0; I < datapoints.size(); I++)
				//for (double[] dataPoint : datapoints.iterator())
				{
					JsonArray dataPoint = datapoints.get(I).getAsJsonArray();
					if (dataPoint.size() < 1)
					{
						validationErrors.addErrorMessage(context + ".timestamp cannot be null or empty.");
						break;
					}
					else if (dataPoint.size() < 2)
					{
						validationErrors.addErrorMessage(dataPointContext + ".value cannot be null or empty.");
						break;
					}
					else
					{
						long timestamp = dataPoint.get(0).getAsLong();
						/*if (metric.validate())
							Validator.isGreaterThanOrEqualTo(validationErrors, dataPointContext + ".value cannot be null or empty.", timestamp, 1);*/

						double value = dataPoint.get(1).getAsDouble();
						if (value % 1 == 0)
							dataPointSet.addDataPoint(new DataPoint(timestamp, (long) value));
						else
							dataPointSet.addDataPoint(new DataPoint(timestamp, value));
					}
					dataPointCount++;
				}
			}

			dataPointCount += dataPointSet.getDataPoints().size();

			if (dataPointSet.getDataPoints().size() > 0)
				datastore.putDataPoints(dataPointSet);
		}

		errors.add(validationErrors);

		return !validationErrors.hasErrors();
	}

	private boolean validateAndAddDataPoints(NewMetric metric, ValidationErrors errors, int count) throws DatastoreException
	{
		ValidationErrors validationErrors = new ValidationErrors();

		String context = "metric[" + count + "]";
		if (metric.validate())
		{
			if (Validator.isNotNullOrEmpty(validationErrors, context + ".name", metric.getName()))
			{
				context = context + "(name=" + metric.getName() + ")";
				Validator.isValidateCharacterSet(validationErrors, context, metric.getName());
			}

			if (metric.getTimestamp() > 0)
				Validator.isNotNullOrEmpty(validationErrors, context + ".value", metric.getValue());
			if (metric.getValue() != null && !metric.getValue().isEmpty())
				Validator.isGreaterThanOrEqualTo(validationErrors, context + ".timestamp", metric.getTimestamp(), 1);

			if (Validator.isGreaterThanOrEqualTo(validationErrors, context + ".tags count", metric.getTags().size(), 1))
			{
				int tagCount = 0;
				String tagContext = context + ".tag[" + tagCount + "]";
				for (Map.Entry<String, String> entry : metric.getTags().entrySet())
				{
					if (Validator.isNotNullOrEmpty(validationErrors, tagContext + ".name", entry.getKey()))
					{
						tagContext = context + ".tag[" + entry.getKey() + "]";
						Validator.isValidateCharacterSet(validationErrors, tagContext, entry.getKey());
					}
					if (Validator.isNotNullOrEmpty(validationErrors, tagContext + ".value", entry.getValue()))
						Validator.isValidateCharacterSet(validationErrors, tagContext + ".value", entry.getValue());

					tagCount++;
				}
			}
		}



		if (!validationErrors.hasErrors())
		{
			DataPointSet dataPointSet = new DataPointSet(metric.getName(), metric.getTags(), Collections.<DataPoint>emptyList());

			if (metric.getTimestamp() > 0 && metric.getValue() != null && !metric.getValue().isEmpty())
			{
				dataPointSet.addDataPoint(new DataPoint(metric.getTimestamp(), metric.getValue()));
			}

			if (metric.getDatapoints() != null && metric.getDatapoints().length > 0)
			{
				int dataPointCount = 0;
				String dataPointContext = context + ".datapoints[" + dataPointCount + "]";
				for (double[] dataPoint : metric.getDatapoints())
				{
					if (dataPoint.length < 1)
					{
						validationErrors.addErrorMessage(context + ".timestamp cannot be null or empty.");
						break;
					}
					else if (dataPoint.length < 2)
					{
						validationErrors.addErrorMessage(dataPointContext + ".value cannot be null or empty.");
						break;
					}
					else
					{
						long timestamp = (long) dataPoint[0];
						if (metric.validate())
							Validator.isGreaterThanOrEqualTo(validationErrors, dataPointContext + ".value cannot be null or empty.", timestamp, 1);

						if (dataPoint[1] % 1 == 0)
							dataPointSet.addDataPoint(new DataPoint(timestamp, (long) dataPoint[1]));
						else
							dataPointSet.addDataPoint(new DataPoint(timestamp, dataPoint[1]));
					}
					dataPointCount++;
				}
			}

			dataPointCount += dataPointSet.getDataPoints().size();

			if (dataPointSet.getDataPoints().size() > 0)
				datastore.putDataPoints(dataPointSet);
		}

		errors.add(validationErrors);

		return !validationErrors.hasErrors();
	}

	@SuppressWarnings({"MismatchedReadAndWriteOfArray", "UnusedDeclaration"})
	private class NewMetric
	{
		private String name;
		private long timestamp = 0;
		private long time = 0;
		private String value;
		private Map<String, String> tags;
		private double[][] datapoints;
		private boolean skip_validate = false;

		private String getName()
		{
			return name;
		}

		public long getTimestamp()
		{
			if (time > 0)
				return time;
			else
				return timestamp;
		}

		public String getValue()
		{
			return value;
		}

		public Map<String, String> getTags()
		{
			return tags != null ? tags : Collections.<String, String>emptyMap();
		}

		private double[][] getDatapoints()
		{
			return datapoints;
		}

		private boolean validate()
		{
			return !skip_validate;
		}
	}
}