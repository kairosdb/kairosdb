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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.validation.metadata.ConstraintDescriptor;

import com.google.inject.name.Named;
import org.apache.bval.constraints.NotEmpty;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.joda.time.DateTimeZone;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.GroupByAware;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.aggregator.TimezoneAware;
import org.kairosdb.core.datastore.Order;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryPlugin;
import org.kairosdb.core.datastore.QueryPluginFactory;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.groupby.GroupByFactory;
import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Inject;


public class QueryParser
{
	private static final Logger logger = LoggerFactory.getLogger(QueryParser.class);

	private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

	private AggregatorFactory m_aggregatorFactory;
	private QueryPluginFactory m_pluginFactory;
	private GroupByFactory m_groupByFactory;
	private Map<Class, Map<String, PropertyDescriptor>> m_descriptorMap;
	private final Object m_descriptorMapLock = new Object();
	private Gson m_gson;

	private static final String DATAPOINT_TTL = "kairosdb.datastore.cassandra.datapoint_ttl";

	@Inject(optional = true)
	@Named(DATAPOINT_TTL)
	private long datapoints_ttl = 3024000;

	@Inject
	public QueryParser(AggregatorFactory aggregatorFactory, GroupByFactory groupByFactory,
	                   QueryPluginFactory pluginFactory)
	{
		m_aggregatorFactory = aggregatorFactory;
		m_groupByFactory = groupByFactory;
		m_pluginFactory = pluginFactory;

		m_descriptorMap = new HashMap<Class, Map<String, PropertyDescriptor>>();

		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapterFactory(new LowercaseEnumTypeAdapterFactory());
		builder.registerTypeAdapter(TimeUnit.class, new TimeUnitDeserializer());
		builder.registerTypeAdapter(DateTimeZone.class, new DateTimeZoneDeserializer());
		builder.registerTypeAdapter(Metric.class, new MetricDeserializer());
		builder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);

		m_gson = builder.create();
	}

	private PropertyDescriptor getPropertyDescriptor(Class objClass, String property) throws IntrospectionException
	{
		synchronized (m_descriptorMapLock)
		{
			Map<String, PropertyDescriptor> propMap = m_descriptorMap.get(objClass);

			if (propMap == null)
			{
				propMap = new HashMap<String, PropertyDescriptor>();
				m_descriptorMap.put(objClass, propMap);

				BeanInfo beanInfo = Introspector.getBeanInfo(objClass);
				PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
				for (PropertyDescriptor descriptor : descriptors)
				{
					propMap.put(getUnderscorePropertyName(descriptor.getName()), descriptor);
				}
			}

			return (propMap.get(property));
		}
	}

	public static String getUnderscorePropertyName(String camelCaseName)
	{
		StringBuilder sb = new StringBuilder();

		for (char c : camelCaseName.toCharArray())
		{
			if (Character.isUpperCase(c))
				sb.append('_').append(Character.toLowerCase(c));
			else
				sb.append(c);
		}

		return (sb.toString());
	}

	private void validateObject(Object object) throws BeanValidationException
	{
		validateObject(object, null);
	}

	private void validateObject(Object object, String context) throws BeanValidationException
	{
		// validate object using the bean validation framework
		Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(object);
		if (!violations.isEmpty())
		{
			throw new BeanValidationException(violations, context);
		}
	}

	public List<QueryMetric> parseQueryMetric(String json) throws QueryException, BeanValidationException
	{
		List<QueryMetric> ret = new ArrayList<QueryMetric>();

		JsonParser parser = new JsonParser();

		JsonObject obj = parser.parse(json).getAsJsonObject();

		Query query;
		try
		{
			query = m_gson.fromJson(obj, Query.class);
			validateObject(query);
		}
		catch (ContextualJsonSyntaxException e)
		{
			throw new BeanValidationException(new SimpleConstraintViolation(e.getContext(), e.getMessage()), "query");
		}

		JsonArray metricsArray = obj.getAsJsonArray("metrics");
		if (metricsArray == null)
		{
			throw new BeanValidationException(new SimpleConstraintViolation("metric[]", "must have a size of at least 1"), "query");
		}

		for (int I = 0; I < metricsArray.size(); I++)
		{
			String context = "query.metric[" + I + "]";
			try
			{
				Metric metric = m_gson.fromJson(metricsArray.get(I), Metric.class);

				validateObject(metric, context);

				long startTime = getStartTime(query);
				QueryMetric queryMetric = new QueryMetric(startTime, query.getCacheTime(),
						metric.getName());
				queryMetric.setExcludeTags(metric.isExcludeTags());
				queryMetric.setLimit(metric.getLimit());

				long endTime = getEndTime(query);
				if (endTime > -1)
					queryMetric.setEndTime(endTime);

				if (queryMetric.getEndTime() < startTime)
					throw new BeanValidationException(new SimpleConstraintViolation("end_time", "must be greater than the start time"), context);

				queryMetric.setCacheString(query.getCacheString() + metric.getCacheString());

				JsonObject jsMetric = metricsArray.get(I).getAsJsonObject();

				JsonElement group_by = jsMetric.get("group_by");
				if (group_by != null)
				{
					JsonArray groupBys = group_by.getAsJsonArray();
					parseGroupBy(context, queryMetric, groupBys);
				}

				JsonElement aggregators = jsMetric.get("aggregators");
				if (aggregators != null)
				{
					JsonArray asJsonArray = aggregators.getAsJsonArray();
					if (asJsonArray.size() > 0)
						parseAggregators(context, queryMetric, asJsonArray, query.getTimeZone());
				}

				JsonElement plugins = jsMetric.get("plugins");
				if (plugins != null)
				{
					JsonArray pluginArray = plugins.getAsJsonArray();
					if (pluginArray.size() > 0)
						parsePlugins(context, queryMetric, pluginArray);
				}

				JsonElement order = jsMetric.get("order");
				if (order != null)
					queryMetric.setOrder(Order.fromString(order.getAsString(), context));

				queryMetric.setTags(metric.getTags());

				ret.add(queryMetric);
			}
			catch (ContextualJsonSyntaxException e)
			{
				throw new BeanValidationException(new SimpleConstraintViolation(e.getContext(), e.getMessage()), context);
			}
		}

		return (ret);
	}

	private void parsePlugins(String context, QueryMetric queryMetric, JsonArray plugins) throws BeanValidationException, QueryException
	{
		for (int I = 0; I < plugins.size(); I++)
		{
			JsonObject pluginJson = plugins.get(I).getAsJsonObject();

			JsonElement name = pluginJson.get("name");
			if (name == null || name.getAsString().isEmpty())
				throw new BeanValidationException(new SimpleConstraintViolation("plugins[" + I + "]", "must have a name"), context);

			String pluginContext = context + ".plugins[" + I + "]";
			String pluginName = name.getAsString();
			QueryPlugin plugin = m_pluginFactory.createQueryPlugin(pluginName);

			if (plugin == null)
				throw new BeanValidationException(new SimpleConstraintViolation(pluginName, "invalid query plugin name"), pluginContext);

			deserializeProperties(pluginContext, pluginJson, pluginName, plugin);

			validateObject(plugin, pluginContext);

			queryMetric.addPlugin(plugin);
		}
	}

	private void parseAggregators(String context, QueryMetric queryMetric,
			JsonArray aggregators, DateTimeZone timeZone) throws QueryException, BeanValidationException
	{
		for (int J = 0; J < aggregators.size(); J++)
		{
			JsonObject jsAggregator = aggregators.get(J).getAsJsonObject();

			JsonElement name = jsAggregator.get("name");
			if (name == null || name.getAsString().isEmpty())
				throw new BeanValidationException(new SimpleConstraintViolation("aggregators[" + J + "]", "must have a name"), context);

			String aggContext = context + ".aggregators[" + J + "]";
			String aggName = name.getAsString();
			Aggregator aggregator = m_aggregatorFactory.createAggregator(aggName);

			if (aggregator == null)
				throw new BeanValidationException(new SimpleConstraintViolation(aggName, "invalid aggregator name"), aggContext);

			//If it is a range aggregator we will default the start time to
			//the start of the query.
			if (aggregator instanceof RangeAggregator)
			{
				RangeAggregator ra = (RangeAggregator) aggregator;
				ra.setStartTime(queryMetric.getStartTime());
			}

			if (aggregator instanceof TimezoneAware)
			{
				TimezoneAware ta = (TimezoneAware) aggregator;
				ta.setTimeZone(timeZone);
			}

			if (aggregator instanceof GroupByAware)
			{
				GroupByAware groupByAware = (GroupByAware) aggregator;
				groupByAware.setGroupBys(queryMetric.getGroupBys());
			}

			deserializeProperties(aggContext, jsAggregator, aggName, aggregator);

			validateObject(aggregator, aggContext);

			queryMetric.addAggregator(aggregator);
		}
	}

	private void parseGroupBy(String context, QueryMetric queryMetric, JsonArray groupBys) throws QueryException, BeanValidationException
	{
		for (int J = 0; J < groupBys.size(); J++)
		{
			String groupContext = "group_by[" + J + "]";
			JsonObject jsGroupBy = groupBys.get(J).getAsJsonObject();

			JsonElement nameElement = jsGroupBy.get("name");
			if (nameElement == null || nameElement.getAsString().isEmpty())
				throw new BeanValidationException(new SimpleConstraintViolation(groupContext, "must have a name"), context);

			String name = nameElement.getAsString();

			GroupBy groupBy = m_groupByFactory.createGroupBy(name);
			if (groupBy == null)
				throw new BeanValidationException(new SimpleConstraintViolation(groupContext + "." + name, "invalid group_by name"), context);

			deserializeProperties(context + "." + groupContext, jsGroupBy, name, groupBy);
			validateObject(groupBy, context + "." + groupContext);

			groupBy.setStartDate(queryMetric.getStartTime());

			queryMetric.addGroupBy(groupBy);
		}
	}

	private void deserializeProperties(String context, JsonObject jsonObject, String name, Object object) throws QueryException, BeanValidationException
	{
		Set<Map.Entry<String, JsonElement>> props = jsonObject.entrySet();
		for (Map.Entry<String, JsonElement> prop : props)
		{
			String property = prop.getKey();
			if (property.equals("name"))
				continue;

			PropertyDescriptor pd = null;
			try
			{
				pd = getPropertyDescriptor(object.getClass(), property);
			}
			catch (IntrospectionException e)
			{
				logger.error("Introspection error on " + object.getClass(), e);
			}

			if (pd == null)
			{
				String msg = "Property '" + property + "' was specified for object '" + name +
						"' but no matching setter was found on '" + object.getClass() + "'";

				throw new QueryException(msg);
			}

			Class propClass = pd.getPropertyType();

			Object propValue;
			try
			{
				propValue = m_gson.fromJson(prop.getValue(), propClass);
				validateObject(propValue, context + "." + property);
			}
			catch (ContextualJsonSyntaxException e)
			{
				throw new BeanValidationException(new SimpleConstraintViolation(e.getContext(), e.getMessage()), context);
			}
			catch(NumberFormatException e)
			{
				throw new BeanValidationException(new SimpleConstraintViolation(property, e.getMessage()), context);
			}

			Method method = pd.getWriteMethod();
			if (method == null)
			{
				String msg = "Property '" + property + "' was specified for object '" + name +
						"' but no matching setter was found on '" + object.getClass().getName() + "'";

				throw new QueryException(msg);
			}

			try
			{
				method.invoke(object, propValue);
			}
			catch (Exception e)
			{
				logger.error("Invocation error: ", e);
				String msg = "Call to " + object.getClass().getName() + ":" + method.getName() +
						" failed with message: " + e.getMessage();

				throw new QueryException(msg);
			}
		}
	}

	private long getStartTime(Query request) throws BeanValidationException
	{
		final long now = System.currentTimeMillis();
		long startTime;
		if (request.getStartAbsolute() != null)
		{
			startTime = request.getStartAbsolute();

		}
		else if (request.getStartRelative() != null)
		{
			startTime = request.getStartRelative().getTimeRelativeTo(now);
		}
		else
		{
			throw new BeanValidationException(new SimpleConstraintViolation("start_time", "relative or absolute time must be set"), "query");
		}
		// ensure that: now-TTL <= startTime <= now
		startTime = Math.max(Math.min(startTime, now), now - datapoints_ttl * 1000);
		// align on one mintue boundaries. Caching takes start time into account.
		return startTime - (startTime % 60_000);
	}

	private long getEndTime(Query request)
	{
		final long now = System.currentTimeMillis();
		long endTime = now;
		if (request.getEndAbsolute() != null) {
			endTime = request.getEndAbsolute();
		} else if (request.getEndRelative() != null) {
			endTime = request.getEndRelative().getTimeRelativeTo(now);
		}
		// ensure that: now-TTL <= endTime <= now
		return Math.max(Math.min(endTime, now), now - datapoints_ttl * 1000);
	}

	//===========================================================================
	private static class Metric
	{
		@NotNull
		@NotEmpty()
		@SerializedName("name")
		private String name;

		@SerializedName("tags")
		private SetMultimap<String, String> tags;

		@SerializedName("exclude_tags")
		private boolean exclude_tags;

		@SerializedName("limit")
		private int limit;

		public Metric(String name, boolean exclude_tags, TreeMultimap<String, String> tags)
		{
			this.name = name;
			this.tags = tags;
			this.exclude_tags = exclude_tags;
			this.limit = 0;
		}

		public String getName()
		{
			return name;
		}

		public int getLimit()
		{
			return limit;
		}

		public void setLimit(int limit)
		{
			this.limit = limit;
		}

		private boolean isExcludeTags()
		{
			return exclude_tags;
		}

		public String getCacheString()
		{
			StringBuilder sb = new StringBuilder();

			sb.append(name).append(":");

			for (Map.Entry<String, String> tagEntry : tags.entries())
			{
				sb.append(tagEntry.getKey()).append("=");
				sb.append(tagEntry.getValue()).append(":");
			}

			return (sb.toString());
		}

		public SetMultimap<String, String> getTags()
		{
			if (tags != null)
			{
				return tags;
			}
			else
			{
				return HashMultimap.create();
			}
		}

	}

	//===========================================================================
	private static class Query
	{
		@SerializedName("start_absolute")
		private Long m_startAbsolute;

		@SerializedName("end_absolute")
		private Long m_endAbsolute;

		@Min(0)
		@SerializedName("cache_time")
		private int cache_time;

		@Valid
		@SerializedName("start_relative")
		private RelativeTime start_relative;

		@Valid
		@SerializedName("end_relative")
		private RelativeTime end_relative;

		@Valid
		@Null
		@SerializedName("time_zone")
		private DateTimeZone m_timeZone;// = DateTimeZone.UTC;;


		public Long getStartAbsolute()
		{
			return m_startAbsolute;
		}

		public Long getEndAbsolute()
		{
			return m_endAbsolute;
		}

		public int getCacheTime()
		{
			return cache_time;
		}

		public RelativeTime getStartRelative()
		{
			return start_relative;
		}

		public RelativeTime getEndRelative()
		{
			return end_relative;
		}

		public DateTimeZone getTimeZone()
		{
			return m_timeZone;
		}

		public String getCacheString()
		{
			StringBuilder sb = new StringBuilder();
			if (m_startAbsolute != null)
				sb.append(m_startAbsolute).append(":");

			if (start_relative != null)
				sb.append(start_relative.toString()).append(":");

			if (m_endAbsolute != null)
				sb.append(m_endAbsolute).append(":");

			if (end_relative != null)
				sb.append(end_relative.toString()).append(":");

			return (sb.toString());
		}

		@Override
		public String toString()
		{
			return "Query{" +
					"startAbsolute='" + m_startAbsolute + '\'' +
					", endAbsolute='" + m_endAbsolute + '\'' +
					", cache_time=" + cache_time +
					", startRelative=" + start_relative +
					", endRelative=" + end_relative +
					'}';
		}
	}

	//===========================================================================
	private static class LowercaseEnumTypeAdapterFactory implements TypeAdapterFactory
	{
		@Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type)

		{
			@SuppressWarnings("unchecked")
			Class<T> rawType = (Class<T>) type.getRawType();
			if (!rawType.isEnum())
			{
				return null;
			}

			final Map<String, T> lowercaseToConstant = new HashMap<String, T>();
			for (T constant : rawType.getEnumConstants())
			{
				lowercaseToConstant.put(toLowercase(constant), constant);
			}

			return new TypeAdapter<T>()
			{
				@Override
                public void write(JsonWriter out, T value) throws IOException
				{
					if (value == null)
					{
						out.nullValue();
					}
					else
					{
						out.value(toLowercase(value));
					}
				}

				@Override
                public T read(JsonReader reader) throws IOException
				{
					if (reader.peek() == JsonToken.NULL)
					{
						reader.nextNull();
						return null;
					}
					else
					{
						return lowercaseToConstant.get(reader.nextString());
					}
				}
			};
		}

		private String toLowercase(Object o)
		{
			return o.toString().toLowerCase(Locale.US);
		}
	}

	//===========================================================================
	private class TimeUnitDeserializer implements JsonDeserializer<TimeUnit>
	{
		@Override
        public TimeUnit deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException
		{
			String unit = json.getAsString();
			TimeUnit tu;

			try
			{
				tu = TimeUnit.from(unit);
			}
			catch (IllegalArgumentException e)
			{
				throw new ContextualJsonSyntaxException(unit,
						"is not a valid time unit, must be one of " + TimeUnit.toValueNames());
			}

			return tu;
		}
	}

	//===========================================================================
	private class DateTimeZoneDeserializer implements JsonDeserializer<DateTimeZone>
	{
		@Override
        public DateTimeZone deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException
		{
			if (json.isJsonNull())
				return null;
			String tz = json.getAsString();
			if (tz.isEmpty()) // defaults to UTC
				return DateTimeZone.UTC;
			DateTimeZone timeZone;

			try
			{
				// check if time zone is valid
				timeZone = DateTimeZone.forID(tz);
			}
			catch (IllegalArgumentException e)
			{
				throw new ContextualJsonSyntaxException(tz,
						"is not a valid time zone, must be one of " + DateTimeZone.getAvailableIDs());
			}
			return timeZone;
		}
	}


	//===========================================================================
	private class MetricDeserializer implements JsonDeserializer<Metric>
	{
		@Override
		public Metric deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext)
				throws JsonParseException
		{
			JsonObject jsonObject = jsonElement.getAsJsonObject();

			String name = null;
			if (jsonObject.get("name") != null)
				name = jsonObject.get("name").getAsString();

			boolean exclude_tags = false;
			if (jsonObject.get("exclude_tags") != null)
				exclude_tags = jsonObject.get("exclude_tags").getAsBoolean();

			TreeMultimap<String, String> tags = TreeMultimap.create();
			JsonElement jeTags = jsonObject.get("tags");
			if (jeTags != null)
			{
				JsonObject joTags = jeTags.getAsJsonObject();
				int count = 0;
				for (Map.Entry<String, JsonElement> tagEntry : joTags.entrySet())
				{
					String context = "tags[" + count + "]";
					if (tagEntry.getKey().isEmpty())
						throw new ContextualJsonSyntaxException(context, "name must not be empty");

					if (tagEntry.getValue().isJsonArray())
					{
						for (JsonElement element : tagEntry.getValue().getAsJsonArray())
						{
							if (element.isJsonNull() || element.getAsString().isEmpty())
								throw new ContextualJsonSyntaxException(context + "." + tagEntry.getKey(), "value must not be null or empty");
							tags.put(tagEntry.getKey(), element.getAsString());
						}
					}
					else
					{
						if (tagEntry.getValue().isJsonNull() || tagEntry.getValue().getAsString().isEmpty())
							throw new ContextualJsonSyntaxException(context + "." + tagEntry.getKey(), "value must not be null or empty");
						tags.put(tagEntry.getKey(), tagEntry.getValue().getAsString());
					}
					count++;
				}
			}

			Metric ret = new Metric(name, exclude_tags, tags);

			JsonElement limit = jsonObject.get("limit");
			if (limit != null)
				ret.setLimit(limit.getAsInt());

			return (ret);
		}
	}

	//===========================================================================
	private static class ContextualJsonSyntaxException extends RuntimeException
	{
		private String context;

		private ContextualJsonSyntaxException(String context, String msg)
		{
			super(msg);
			this.context = context;
		}

		private String getContext()
		{
			return context;
		}
	}

	//===========================================================================
	public static class SimpleConstraintViolation implements ConstraintViolation<Object>
	{
		private String message;
		private String context;

		public SimpleConstraintViolation(String context, String message)
		{
			this.message = message;
			this.context = context;
		}

		@Override
		public String getMessage()
		{
			return message;
		}

		@Override
		public String getMessageTemplate()
		{
			return null;
		}

		@Override
		public Object getRootBean()
		{
			return null;
		}

		@Override
		public Class<Object> getRootBeanClass()
		{
			return null;
		}

		@Override
		public Object getLeafBean()
		{
			return null;
		}

		@Override
		public Path getPropertyPath()
		{
			return new SimplePath(context);
		}

		@Override
		public Object getInvalidValue()
		{
			return null;
		}

		@Override
		public ConstraintDescriptor<?> getConstraintDescriptor()
		{
			return null;
		}

        @Override
        public Object[] getExecutableParameters() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Object getExecutableReturnValue() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public <U> U unwrap(Class<U> type) {
            // TODO Auto-generated method stub
            return null;
        }
	}

	private static class SimplePath implements Path
	{
		private String context;

		private SimplePath(String context)
		{
			this.context = context;
		}

		@Override
		public Iterator<Node> iterator()
		{
			return null;
		}

		@Override
		public String toString()
		{
			return context;
		}
	}

}
