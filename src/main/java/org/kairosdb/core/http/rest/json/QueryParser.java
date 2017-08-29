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

package org.kairosdb.core.http.rest.json;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Inject;
import org.apache.bval.constraints.NotEmpty;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.joda.time.DateTimeZone;
import org.kairosdb.core.aggregator.*;
import org.kairosdb.core.annotation.Feature;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.QueryException;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.FeatureProcessor;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;
import org.kairosdb.rollup.Rollup;
import org.kairosdb.rollup.RollupTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.constraints.NotNull;
import javax.validation.metadata.ConstraintDescriptor;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;


public class QueryParser
{
    protected static final Logger logger = LoggerFactory.getLogger(QueryParser.class);
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    private FeatureProcessor m_processingChain;
    private QueryPluginFactory m_pluginFactory;

    private Gson m_gson;
    private Map<Class, Map<String, PropertyDescriptor>> m_descriptorMap;
    private final Object m_descriptorMapLock = new Object();

    @Inject
    public QueryParser(FeatureProcessor processingChain, QueryPluginFactory pluginFactory)
    {
        m_processingChain = processingChain;
        m_pluginFactory = pluginFactory;

        m_descriptorMap = new HashMap<>();

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapterFactory(new LowercaseEnumTypeAdapterFactory());
        gsonBuilder.registerTypeAdapter(TimeUnit.class, new TimeUnitDeserializer());
        gsonBuilder.registerTypeAdapter(TrimAggregator.Trim.class, new TrimDeserializer());
        gsonBuilder.registerTypeAdapter(FilterAggregator.FilterOperation.class, new FilterOperationDeserializer());
        gsonBuilder.registerTypeAdapter(DateTimeZone.class, new DateTimeZoneDeserializer());
        gsonBuilder.registerTypeAdapter(Metric.class, new MetricDeserializer());
        gsonBuilder.registerTypeAdapter(SetMultimap.class, new SetMultimapDeserializer());
        gsonBuilder.registerTypeAdapter(RelativeTime.class, new RelativeTimeSerializer());
        gsonBuilder.registerTypeAdapter(SetMultimap.class, new SetMultimapSerializer());
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);

        m_gson = gsonBuilder.create();
    }

    public Gson getGson()
    {
        return m_gson;
    }

    static String getUnderscorePropertyName(String camelCaseName)
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

    private PropertyDescriptor getPropertyDescriptor(Class objClass, String property) throws IntrospectionException
    {
        synchronized (m_descriptorMapLock)
        {
            Map<String, PropertyDescriptor> propMap = m_descriptorMap.get(objClass);

            if (propMap == null)
            {
                propMap = new HashMap<>();
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

    private long getStartTime(Query request, String context) throws BeanValidationException
    {
        if (request.getStartAbsolute() != null)
        {
            return request.getStartAbsolute();
        } else if (request.getStartRelative() != null)
        {
            return request.getStartRelative().getTimeRelativeTo(System.currentTimeMillis());
        } else
        {
            throw new BeanValidationException(new SimpleConstraintViolation("start_time", "relative or absolute time must be set"), context);
        }
    }

    private long getEndTime(Query request)
    {
        if (request.getEndAbsolute() != null)
            return request.getEndAbsolute();
        else if (request.getEndRelative() != null)
            return request.getEndRelative().getTimeRelativeTo(System.currentTimeMillis());
        return -1;
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


    public Query parseQueryMetric(String json) throws QueryException, BeanValidationException
    {
        JsonParser parser = new JsonParser();
        JsonObject obj = parser.parse(json).getAsJsonObject();
        return parseQueryMetric(obj);
    }

	private Query parseQueryMetric(JsonObject obj) throws QueryException, BeanValidationException
	{
		return parseQueryMetric(obj, "");
	}

	private Query parseQueryMetric(JsonObject obj, String contextPrefix) throws QueryException, BeanValidationException
	{
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

		//Parses plugins for the entire query
		//Initial use was for post processing plugins
		JsonElement queryPlugins = obj.get("plugins");
		if (queryPlugins != null)
		{
			JsonArray pluginArray = queryPlugins.getAsJsonArray();
			if (pluginArray.size() > 0)
				parsePlugins("", query, pluginArray);
		}JsonArray metricsArray = obj.getAsJsonArray("metrics");
		if (metricsArray == null)
		{
			throw new BeanValidationException(new SimpleConstraintViolation("metric[]", "must have a size of at least 1"), contextPrefix + "query");
		}

        for (int I = 0; I < metricsArray.size(); I++)
        {
            String context = (!contextPrefix.isEmpty() ? contextPrefix + "." : contextPrefix) + "query.metric[" + I + "]";
            try
            {
                Metric metric = m_gson.fromJson(metricsArray.get(I), Metric.class);

                validateObject(metric, context);

                long startTime = getStartTime(query, context);
                QueryMetric queryMetric = new QueryMetric(startTime, query.getCacheTime(), metric.getName());
                queryMetric.setExcludeTags(metric.isExcludeTags());
                queryMetric.setLimit(metric.getLimit());

                long endTime = getEndTime(query);
                if (endTime > -1)
                    queryMetric.setEndTime(endTime);

                if (queryMetric.getEndTime() < startTime)
                    throw new BeanValidationException(new SimpleConstraintViolation("end_time", "must be greater than the start time"), context);

                queryMetric.setCacheString(query.getCacheString() + metric.getCacheString());

                JsonObject jsMetric = metricsArray.get(I).getAsJsonObject();

                for (FeatureProcessingFactory<?> factory : m_processingChain.getFeatureProcessingFactories())
                {
                    String factoryName = factory.getClass().getAnnotation(Feature.class).name();

                    JsonElement queryProcessor = jsMetric.get(factoryName);
                    if (queryProcessor != null)
                    {
                        JsonArray queryProcessorArray = queryProcessor.getAsJsonArray();
                        parseQueryProcessor(context, factoryName,
                                queryProcessorArray, factory.getFeature(),
                                queryMetric, query.getTimeZone());
                    }
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

				query.addQueryMetric(queryMetric);
			}
			catch (ContextualJsonSyntaxException e)
			{
				throw new BeanValidationException(new SimpleConstraintViolation(e.getContext(), e.getMessage()), context);
			}
		}

        return (query);
    }

    private void parseSpecificQueryProcessor(Object queryProcessor, QueryMetric queryMetric, DateTimeZone timeZone)
    {
        if (queryProcessor instanceof RangeAggregator)
        {
            RangeAggregator ra = (RangeAggregator) queryProcessor;
            ra.setStartTime(queryMetric.getStartTime());
            ra.setEndTime(queryMetric.getEndTime());
        }

        if (queryProcessor instanceof TimezoneAware)
        {
            TimezoneAware ta = (TimezoneAware) queryProcessor;
            ta.setTimeZone(timeZone);
        }

        if (queryProcessor instanceof GroupByAware)
        {
            GroupByAware groupByAware = (GroupByAware) queryProcessor;
            groupByAware.setGroupBys(queryMetric.getGroupBys());
        }

        if (queryProcessor instanceof GroupBy)
        {
            GroupBy groupBy = (GroupBy) queryProcessor;
            groupBy.setStartDate(queryMetric.getStartTime());
        }
    }

    private void addQueryProcessorToMetric(Object queryProcessor, QueryMetric queryMetric)
    {
        if (queryProcessor instanceof Aggregator)
            queryMetric.addAggregator((Aggregator) queryProcessor);
        if (queryProcessor instanceof GroupBy)
            queryMetric.addGroupBy((GroupBy) queryProcessor);
    }

    private void parseQueryProcessor(String context, String queryProcessorFamilyName,
          JsonArray queryProcessors, Class<?> queryProcessorFamilyType,
          QueryMetric queryMetric, DateTimeZone dateTimeZone)
            throws BeanValidationException, QueryException
    {
        for (int J = 0; J < queryProcessors.size(); J++)
        {
            JsonObject jsQueryProcessor = queryProcessors.get(J).getAsJsonObject();

            JsonElement name = jsQueryProcessor.get("name");
            if (name == null || name.getAsString().isEmpty())
                throw new BeanValidationException(new SimpleConstraintViolation(queryProcessorFamilyName + "[" + J + "]", "must have a name"), context);

            String qpContext = context + "." + queryProcessorFamilyName + "[" + J + "]";
            String qpName = name.getAsString();
            Object queryProcessor = m_processingChain.getFeatureProcessingFactory(queryProcessorFamilyType).createFeatureProcessor(qpName);

            if (queryProcessor == null)
                throw new BeanValidationException(new SimpleConstraintViolation(qpName, "invalid " + queryProcessorFamilyName + " name"), qpContext);

            parseSpecificQueryProcessor(queryProcessor, queryMetric, dateTimeZone);
            deserializeProperties(qpContext, jsQueryProcessor, qpName, queryProcessor);
            validateObject(queryProcessor, qpContext);
            addQueryProcessorToMetric(queryProcessor, queryMetric);
        }
    }

    public List<RollupTask> parseRollupTasks(String json) throws BeanValidationException, QueryException
    {
        List<RollupTask> tasks = new ArrayList<>();
        JsonParser parser = new JsonParser();
        JsonArray rollupTasks = parser.parse(json).getAsJsonArray();
        for (int i = 0; i < rollupTasks.size(); i++)
        {
            JsonObject taskObject = rollupTasks.get(i).getAsJsonObject();
            RollupTask task = parseRollupTask(taskObject, "tasks[" + i + "]");
            task.addJson(taskObject.toString().replaceAll("\\n", ""));
            tasks.add(task);
        }

        return tasks;
    }

    public RollupTask parseRollupTask(String json) throws BeanValidationException, QueryException
    {
        JsonParser parser = new JsonParser();
        JsonObject taskObject = parser.parse(json).getAsJsonObject();
        RollupTask task = parseRollupTask(taskObject, "");
        task.addJson(taskObject.toString().replaceAll("\\n", ""));
        return task;
    }

    private RollupTask parseRollupTask(JsonObject rollupTask, String context) throws BeanValidationException, QueryException
    {
        RollupTask task = m_gson.fromJson(rollupTask.getAsJsonObject(), RollupTask.class);

        validateObject(task);

        JsonArray rollups = rollupTask.getAsJsonObject().getAsJsonArray("rollups");
        if (rollups != null)
        {
            for (int j = 0; j < rollups.size(); j++)
            {
                JsonObject rollupObject = rollups.get(j).getAsJsonObject();
                Rollup rollup = m_gson.fromJson(rollupObject, Rollup.class);

                context = context + "rollup[" + j + "]";
                validateObject(rollup, context);

                JsonObject queryObject = rollupObject.getAsJsonObject("query");
                List<QueryMetric> queries = parseQueryMetric(queryObject, context).getQueryMetrics();

                for (int k = 0; k < queries.size(); k++)
                {
                    QueryMetric query = queries.get(k);
                    context += ".query[" + k + "]";
                    validateHasRangeAggregator(query, context);

                    // Add aggregators needed for rollups
                    SaveAsAggregator saveAsAggregator = (SaveAsAggregator) m_processingChain.getFeatureProcessingFactory(Aggregator.class).createFeatureProcessor("save_as");
                    saveAsAggregator.setMetricName(rollup.getSaveAs());
                    saveAsAggregator.setGroupBys(query.getGroupBys());

                    TrimAggregator trimAggregator = (TrimAggregator) m_processingChain.getFeatureProcessingFactory(Aggregator.class).createFeatureProcessor("trim");
                    trimAggregator.setTrim(TrimAggregator.Trim.LAST);

                    query.addAggregator(saveAsAggregator);
                    query.addAggregator(trimAggregator);
                }

                rollup.addQueries(queries);
                task.addRollup(rollup);
            }
        }

        return task;
    }

	private void validateHasRangeAggregator(QueryMetric query, String context) throws BeanValidationException
	{
		boolean hasRangeAggregator = false;
		for (Aggregator aggregator : query.getAggregators())
		{
			if (aggregator instanceof RangeAggregator)
			{
				hasRangeAggregator = true;
				break;
			}
		}

		if (!hasRangeAggregator)
		{
			throw new BeanValidationException(new SimpleConstraintViolation("aggregator", "At least one aggregator must be a range aggregator"), context);
		}
	}

	private void parsePlugins(String context, PluggableQuery queryMetric, JsonArray plugins) throws BeanValidationException, QueryException
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
            } catch (IntrospectionException e)
            {
                logger.error("Introspection error on " + object.getClass(), e);
            }

            if (pd == null)
            {
                String msg = "Property '" + property + "' was specified for object '" + name +
                        "' but no matching setter was found on '" + object.getClass() + "'";

                throw new QueryException(msg);
            }

            Class<?> propClass = pd.getPropertyType();

            Object propValue;
            try
            {
                propValue = m_gson.fromJson(prop.getValue(), propClass);
                validateObject(propValue, context + "." + property);
            } catch (ContextualJsonSyntaxException e)
            {
                throw new BeanValidationException(new SimpleConstraintViolation(e.getContext(), e.getMessage()), context);
            } catch (NumberFormatException e)
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
            } catch (Exception e)
            {
                logger.error("Invocation error: ", e);
                String msg = "Call to " + object.getClass().getName() + ":" + method.getName() +
                        " failed with message: " + e.getMessage();

                throw new QueryException(msg);
            }
        }
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

        String getCacheString()
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
            } else
            {
                return HashMultimap.create();
            }
        }

    }

	//===========================================================================

	private static class LowercaseEnumTypeAdapterFactory implements TypeAdapterFactory
	{
		public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type)

        {
            @SuppressWarnings("unchecked")
            Class<T> rawType = (Class<T>) type.getRawType();
            if (!rawType.isEnum())
            {
                return null;
            }

            final Map<String, T> lowercaseToConstant = new HashMap<>();
            for (T constant : rawType.getEnumConstants())
            {
                lowercaseToConstant.put(toLowercase(constant), constant);
            }

            return new TypeAdapter<T>()
            {
                public void write(JsonWriter out, T value) throws IOException
                {
                    if (value == null)
                    {
                        out.nullValue();
                    } else
                    {
                        out.value(toLowercase(value));
                    }
                }

                public T read(JsonReader reader) throws IOException
                {
                    if (reader.peek() == JsonToken.NULL)
                    {
                        reader.nextNull();
                        return null;
                    } else
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
    private static class TimeUnitDeserializer implements JsonDeserializer<TimeUnit>
    {
        public TimeUnit deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException
        {
            String unit = json.getAsString();
            TimeUnit tu;

            try
            {
                tu = TimeUnit.from(unit);
            } catch (IllegalArgumentException e)
            {
                throw new ContextualJsonSyntaxException(unit,
                        "is not a valid time unit, must be one of " + TimeUnit.toValueNames());
            }

            return tu;
        }
    }

    private static abstract class EnumDeserializer<TEnum extends Enum<TEnum>> implements JsonDeserializer<TEnum>
    {
        TEnum genericDeserializer(JsonElement json, Class<TEnum> type)
                throws JsonParseException
        {
            String jsValue = json.getAsString();
            TEnum[] enumDefinition = type.getEnumConstants();

            for (TEnum value : enumDefinition)
                if (value.toString().equalsIgnoreCase(jsValue))
                    return value;

            StringBuilder values = new StringBuilder("is not a valid trim type, must be ");
            for (int i = 0; i < enumDefinition.length; i++)
            {
                values.append("'").append(enumDefinition[i].toString().toLowerCase()).append("'");

                if (i < enumDefinition.length - 2)
                    values.append(", ");
                else if (i < enumDefinition.length - 1)
                    values.append(" or ");
                else
                    values.append(".");
            }
            throw new ContextualJsonSyntaxException(jsValue, values.toString());
        }
    }

    private static class TrimDeserializer extends EnumDeserializer<TrimAggregator.Trim>
    {
        public TrimAggregator.Trim deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException
        {
            return genericDeserializer(json, TrimAggregator.Trim.class);
        }
    }

    private static class FilterOperationDeserializer extends EnumDeserializer<FilterAggregator.FilterOperation>
    {
        public FilterAggregator.FilterOperation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException
        {
            return genericDeserializer(json, FilterAggregator.FilterOperation.class);
        }
    }

    //===========================================================================
    private static class DateTimeZoneDeserializer implements JsonDeserializer<DateTimeZone>
    {
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
            } catch (IllegalArgumentException e)
            {
                throw new ContextualJsonSyntaxException(tz,
                        "is not a valid time zone, must be one of " + DateTimeZone.getAvailableIDs());
            }
            return timeZone;
        }
    }


    //===========================================================================
    private static class MetricDeserializer implements JsonDeserializer<Metric>
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
                    } else
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
