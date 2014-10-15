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

import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.Validator;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Originally used Jackson to parse, but this approach failed for a very large JSON because
 * everything was in memory and we would run out of memory. This parser adds metrics as it walks
 * through the stream.
 */
public class JsonMetricParser
{
	private final KairosDatastore datastore;
	private final Reader inputStream;
	private final Gson gson;
	private final KairosDataPointFactory dataPointFactory;
    private final ValidationErrors validationErrors = new ValidationErrors();

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

	public JsonMetricParser(KairosDatastore datastore, Reader stream, Gson gson,
			KairosDataPointFactory dataPointFactory)
	{
		this.datastore = checkNotNull(datastore);
		this.inputStream = checkNotNull(stream);
		this.gson = gson;
		this.dataPointFactory = dataPointFactory;
	}

    public ValidationErrors parseWhenH2IsUsed() throws IOException, DatastoreException
    {
        return null;
    }
	public ValidationErrors parse() throws IOException, DatastoreException
	{
		long start = System.currentTimeMillis();

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
						NewMetric metric = parseMetric(reader);
						if (validate(metric, metricCount)) {
                            if (datastore.h2DatabaseUsed()) {
                                addDataPointsInBulk(metric, metricCount);
                            }
                            addDataPoints(metric, metricCount);
                        }
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
                if (validate(metric, 0)) {
                    if (datastore.h2DatabaseUsed()) {
                        addDataPointsInBulk(metric, 0);
                    }
                    addDataPoints(metric, 0);
                }
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

	private class Context
	{
		private int m_count;
		private String m_name;
		private String m_attribute;

		public Context(int count)
		{
			m_count = count;
		}

		private Context setCount(int count)
		{
			m_count = count;
			return (this);
		}

		private Context setName(String name)
		{
			m_name = name;
			m_attribute = null;
			return (this);
		}

		private Context setAttribute(String attribute)
		{
			m_attribute = attribute;
			return (this);
		}

		public String toString()
		{
			StringBuilder sb = new StringBuilder();
			sb.append("metric[").append(m_count).append("]");
			if (m_name != null)
				sb.append("(name=").append(m_name).append(")");

			if (m_attribute != null)
				sb.append(".").append(m_attribute);

			return (sb.toString());
		}
	}

	private class SubContext
	{
		private Context m_context;
		private String m_contextName;
		private int m_count;
		private String m_name;
		private String m_attribute;

		public SubContext(Context context, String contextName)
		{
			m_context = context;
			m_contextName = contextName;
		}

		private SubContext setCount(int count)
		{
			m_count = count;
			m_name = null;
			m_attribute = null;
			return (this);
		}

		private SubContext setName(String name)
		{
			m_name = name;
			m_attribute = null;
			return (this);
		}

		private SubContext setAttribute(String attribute)
		{
			m_attribute = attribute;
			return (this);
		}

		public String toString()
		{
			StringBuilder sb = new StringBuilder();
			sb.append(m_context).append(".").append(m_contextName).append("[");
			if (m_name != null)
				sb.append(m_name);
			else
				sb.append(m_count);
			sb.append("]");

			if (m_attribute != null)
				sb.append(".").append(m_attribute);

			return (sb.toString());
		}
	}

	private String findType(JsonElement value)
	{
		String v = value.getAsString();

		if (!v.contains("."))
			return "long";
		else
			return "double";
	}

	private boolean validate(NewMetric metric, int count) throws DatastoreException, IOException {

        Context context = new Context(count);
        if (metric.validate()) {
            if (Validator.isNotNullOrEmpty(validationErrors, context.setAttribute("name"), metric.getName())) {
                context.setName(metric.getName());
                Validator.isValidateCharacterSet(validationErrors, context, metric.getName());
            }

            if (metric.getTimestamp() > 0)
                Validator.isNotNullOrEmpty(validationErrors, context.setAttribute("value"), metric.getValue());
            else if (metric.getValue() != null && !metric.getValue().isJsonNull())
                Validator.isGreaterThanOrEqualTo(validationErrors, context.setAttribute("timestamp"), metric.getTimestamp(), 1);


            if (Validator.isGreaterThanOrEqualTo(validationErrors, context.setAttribute("tags count"), metric.getTags().size(), 1)) {
                int tagCount = 0;
                SubContext tagContext = new SubContext(context.setAttribute(null), "tag");

                for (Map.Entry<String, String> entry : metric.getTags().entrySet()) {
                    tagContext.setCount(tagCount);
                    if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("name"), entry.getKey())) {
                        tagContext.setName(entry.getKey());
                        Validator.isValidateCharacterSet(validationErrors, tagContext, entry.getKey());
                    }
                    if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("value"), entry.getValue()))
                        Validator.isValidateCharacterSet(validationErrors, tagContext, entry.getValue());

                    tagCount++;
                }
            }
        }

        if (metric.getTimestamp() > 0 && metric.getValue() != null) {
            String type = metric.getType();
            if (type == null)
                type = findType(metric.getValue());

            if (!dataPointFactory.isRegisteredType(type)) {
                validationErrors.addErrorMessage("Unregistered data point type '" + type + "'");
            }

        }
        SubContext dataPointContext = new SubContext(context, "datapoints");
        for (JsonElement[] dataPoint : metric.getDatapoints()) {
             if (dataPoint.length < 1) {
                validationErrors.addErrorMessage(dataPointContext.setAttribute("timestamp") + " cannot be null or empty.");

            } else if (dataPoint.length < 2) {
                validationErrors.addErrorMessage(dataPointContext.setAttribute("value") + " cannot be null or empty.");

            }
            if (dataPoint.length < 1) {
                validationErrors.addErrorMessage(dataPointContext.setAttribute("timestamp") + " cannot be null or empty.");

            } if (dataPoint.length < 2) {
                validationErrors.addErrorMessage(dataPointContext.setAttribute("value") + " cannot be null or empty.");
                }
        }

        return !validationErrors.hasErrors();
    }
    private void addDataPoints(NewMetric metric, int count) throws DatastoreException, IOException
    {
        {
            //DataPointSet dataPointSet = new DataPointSet(metric.getName(), metric.getTags(), Collections.<DataPoint>emptyList());
            ImmutableSortedMap<String, String> tags = ImmutableSortedMap.copyOf(metric.getTags());
            Context context = new Context(count);

            if (metric.getTimestamp() > 0 && metric.getValue() != null) {
                String type = metric.getType();
                if (type == null)
                    type = findType(metric.getValue());
                    datastore.putDataPoint(metric.getName(), tags, dataPointFactory.createDataPoint(
                            type, metric.getTimestamp(), metric.getValue()));
            }

            if (metric.getDatapoints() != null && metric.getDatapoints().length > 0) {
                int contextCount = 0;
                SubContext dataPointContext = new SubContext(context, "datapoints");
                for (JsonElement[] dataPoint : metric.getDatapoints()) {
                    dataPointContext.setCount(contextCount);

                        long timestamp = 0L;
                        if (!dataPoint[0].isJsonNull())
                            timestamp = dataPoint[0].getAsLong();

                        if (metric.validate() && !Validator.isGreaterThanOrEqualTo(validationErrors, dataPointContext.setAttribute("value") + " cannot be null or empty,", timestamp, 1))
                            continue;

                        String type = metric.getType();
                        if (dataPoint.length > 2)
                            type = dataPoint[2].getAsString();

                        if (!Validator.isNotNullOrEmpty(validationErrors, dataPointContext.setAttribute("value"), dataPoint[1]))
                            continue;

                        if (type == null)
                            type = findType(dataPoint[1]);

                        if (!dataPointFactory.isRegisteredType(type)) {
                            validationErrors.addErrorMessage("Unregistered data point type '" + type + "'");
                            continue;
                        }

                        datastore.putDataPoint(metric.getName(), tags,
                                dataPointFactory.createDataPoint(type, timestamp, dataPoint[1]));
                        dataPointCount++;

                    contextCount++;
                }
            }
        }


	}

    private void addDataPointsInBulk(NewMetric metric, int count) throws DatastoreException, IOException
    {
        {
            //DataPointSet dataPointSet = new DataPointSet(metric.getName(), metric.getTags(), Collections.<DataPoint>emptyList());
            ImmutableSortedMap<String, String> tags = ImmutableSortedMap.copyOf(metric.getTags());
            Context context = new Context(count);

            if (metric.getTimestamp() > 0 && metric.getValue() != null) {
                String type = metric.getType();
                if (type == null)
                    type = findType(metric.getValue());
                datastore.putDataPoint(metric.getName(), tags, dataPointFactory.createDataPoint(
                        type, metric.getTimestamp(), metric.getValue()));
            }

            if (metric.getDatapoints() != null && metric.getDatapoints().length > 0) {
                int contextCount = 0;
                String type = null;
                long timestamp = 0L;
                SubContext dataPointContext = new SubContext(context, "datapoints");
                Map<Long, Map<JsonElement, String>> dataPoints = new HashMap<Long, Map<JsonElement, String>>();
                for (JsonElement[] dataPoint : metric.getDatapoints()) {
                    dataPointContext.setCount(contextCount);

                    timestamp = 0L;
                    if (!dataPoint[0].isJsonNull())
                        timestamp = dataPoint[0].getAsLong();

                    if (metric.validate() && !Validator.isGreaterThanOrEqualTo(validationErrors, dataPointContext.setAttribute("value") + " cannot be null or empty,", timestamp, 1))
                        continue;

                    type = metric.getType();
                    if (dataPoint.length > 2)
                        type = dataPoint[2].getAsString();

                    if (!Validator.isNotNullOrEmpty(validationErrors, dataPointContext.setAttribute("value"), dataPoint[1]))
                        continue;

                    if (type == null)
                        type = findType(dataPoint[1]);

                    if (!dataPointFactory.isRegisteredType(type)) {
                        validationErrors.addErrorMessage("Unregistered data point type '" + type + "'");
                        continue;
                    }
                    Map<JsonElement, String> jsonTypeMap = new HashMap<JsonElement, String>();
                    jsonTypeMap.put(dataPoint[1], type);
                    dataPoints.put(timestamp, jsonTypeMap);

                    contextCount++;
                }
                datastore.putDataPoints(metric.getName(), tags,
                        dataPointFactory.createDataPoints(dataPoints));
                dataPointCount++;
            }
        }


    }

	@SuppressWarnings({"MismatchedReadAndWriteOfArray", "UnusedDeclaration"})
	private class NewMetric
	{
		private String name;
		private long timestamp = 0;
		private long time = 0;
		private JsonElement value;
		private Map<String, String> tags;
		private JsonElement[][] datapoints;
		private boolean skip_validate = false;
		private String type;

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

		public JsonElement getValue()
		{
			return value;
		}

		public Map<String, String> getTags()
		{
			return tags != null ? tags : Collections.<String, String>emptyMap();
		}

		private JsonElement[][] getDatapoints()
		{
			return datapoints;
		}

		private boolean validate()
		{
			return !skip_validate;
		}

		public String getType() { return type; }
	}
}