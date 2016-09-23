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
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.Util;
import org.kairosdb.util.ValidationException;
import org.kairosdb.util.Validator;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Originally used Jackson to parse, but this approach failed for a very large JSON because
 * everything was in memory and we would run out of memory. This parser adds metrics as it walks
 * through the stream.
 */
public class DataPointsParser
{
	private final KairosDatastore datastore;
	private final Reader inputStream;
	private final Gson gson;
	private final KairosDataPointFactory dataPointFactory;

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

	public DataPointsParser(KairosDatastore datastore, Reader stream, Gson gson,
			KairosDataPointFactory dataPointFactory)
	{
		this.datastore = checkNotNull(datastore);
		this.inputStream = checkNotNull(stream);
		this.gson = gson;
		this.dataPointFactory = dataPointFactory;
	}

	public ValidationErrors parse() throws IOException, DatastoreException
	{
		long start = System.currentTimeMillis();
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
						NewMetric metric = parseMetric(reader);
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

	private class Context
	{
		private int m_count;
		private String m_name;
		private String m_attribute;

		public Context(int count)
		{
			m_count = count;
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

	private String findType(JsonElement value) throws ValidationException
	{
		if (!value.isJsonPrimitive()){
			throw new ValidationException("value is an invalid type");
		}

		JsonPrimitive primitiveValue = (JsonPrimitive) value;
		if (primitiveValue.isNumber() || (primitiveValue.isString() && Util.isNumber(value.getAsString())))
		{
			String v = value.getAsString();

			if (!v.contains("."))
			{
				return "long";
			}
			else
			{
				return "double";
			}
		}
		else
			return "string";
	}

	private boolean validateAndAddDataPoints(NewMetric metric, ValidationErrors errors, int count) throws DatastoreException, IOException
	{
		ValidationErrors validationErrors = new ValidationErrors();

		Context context = new Context(count);
		if (metric.validate())
		{
			if (Validator.isNotNullOrEmpty(validationErrors, context.setAttribute("name"), metric.getName()))
			{
				context.setName(metric.getName());
				//Validator.isValidateCharacterSet(validationErrors, context, metric.getName());
			}

			if (metric.getTimestamp() != null)
				Validator.isNotNullOrEmpty(validationErrors, context.setAttribute("value"), metric.getValue());
			else if (metric.getValue() != null && !metric.getValue().isJsonNull())
				Validator.isNotNull(validationErrors, context.setAttribute("timestamp"), metric.getTimestamp());
			//				Validator.isGreaterThanOrEqualTo(validationErrors, context.setAttribute("timestamp"), metric.getTimestamp(), 1);


			if (Validator.isGreaterThanOrEqualTo(validationErrors, context.setAttribute("tags count"), metric.getTags().size(), 1))
			{
				int tagCount = 0;
				SubContext tagContext = new SubContext(context.setAttribute(null), "tag");

				for (Map.Entry<String, String> entry : metric.getTags().entrySet())
				{
					tagContext.setCount(tagCount);
					if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("name"), entry.getKey()))
					{
						tagContext.setName(entry.getKey());
						Validator.isValidateCharacterSet(validationErrors, tagContext, entry.getKey());
					}
					if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("value"), entry.getValue()))
						Validator.isValidateCharacterSet(validationErrors, tagContext, entry.getValue());

					tagCount++;
				}
			}
		}


		if (!validationErrors.hasErrors())
		{
			ImmutableSortedMap<String, String> tags = ImmutableSortedMap.copyOf(metric.getTags());

			if (metric.getTimestamp() != null && metric.getValue() != null)
			{
				String type = metric.getType();
				if (type == null) {
                    try {
                        type = findType(metric.getValue());
                    }
                    catch (ValidationException e) {
                        validationErrors.addErrorMessage(context + " " + e.getMessage());
                    }
                }

                if (type != null) {
                    if (dataPointFactory.isRegisteredType(type)) {
                        datastore.putDataPoint(metric.getName(), tags, dataPointFactory.createDataPoint(
                                type, metric.getTimestamp(), metric.getValue()), metric.getTtl());
                        dataPointCount++;
                    }
                    else {
                        validationErrors.addErrorMessage("Unregistered data point type '" + type + "'");
                    }
                }
            }

			if (metric.getDatapoints() != null && metric.getDatapoints().length > 0)
			{
				int contextCount = 0;
				SubContext dataPointContext = new SubContext(context, "datapoints");
				for (JsonElement[] dataPoint : metric.getDatapoints())
				{
					dataPointContext.setCount(contextCount);
					if (dataPoint.length < 1)
					{
						validationErrors.addErrorMessage(dataPointContext.setAttribute("timestamp") +" cannot be null or empty.");
						continue;
					}
					else if (dataPoint.length < 2)
					{
						validationErrors.addErrorMessage(dataPointContext.setAttribute("value") + " cannot be null or empty.");
						continue;
					}
					else
					{
						Long timestamp = null;
						if (!dataPoint[0].isJsonNull())
							timestamp = dataPoint[0].getAsLong();

						if (metric.validate() && !Validator.isNotNull(validationErrors, dataPointContext.setAttribute("timestamp"), timestamp))
							continue;

						String type = metric.getType();
						if (dataPoint.length > 2)
							type = dataPoint[2].getAsString();

						if (!Validator.isNotNullOrEmpty(validationErrors, dataPointContext.setAttribute("value"), dataPoint[1]))
							continue;

						if (type == null) {
                            try {
                                type = findType(dataPoint[1]);
                            }
                            catch (ValidationException e) {
                                validationErrors.addErrorMessage(context + " " + e.getMessage());
                                continue;
                            }
                        }

						if (!dataPointFactory.isRegisteredType(type))
						{
							validationErrors.addErrorMessage("Unregistered data point type '"+type+"'");
							continue;
						}

						datastore.putDataPoint(metric.getName(), tags,
								dataPointFactory.createDataPoint(type, timestamp, dataPoint[1]), metric.getTtl());
						dataPointCount ++;
					}
					contextCount++;
				}
			}
		}

		errors.add(validationErrors);

		return !validationErrors.hasErrors();
	}

	@SuppressWarnings({"MismatchedReadAndWriteOfArray", "UnusedDeclaration"})
	private class NewMetric
	{
		private String name;
		private Long timestamp = null;
		private Long time = null;
		private JsonElement value;
		private Map<String, String> tags;
		private JsonElement[][] datapoints;
		private boolean skip_validate = false;
		private String type;
		private int ttl = 0;

		private String getName()
		{
			return name;
		}

		public Long getTimestamp()
		{
			if (time != null)
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

		public int getTtl()
		{
			return ttl;
		}
	}
}