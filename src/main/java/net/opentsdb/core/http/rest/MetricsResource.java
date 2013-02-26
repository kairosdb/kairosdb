// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>

package net.opentsdb.core.http.rest;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPointSet;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.Datastore;
import net.opentsdb.core.datastore.QueryMetric;
import net.opentsdb.core.formatter.DataFormatter;
import net.opentsdb.core.formatter.JsonFormatter;
import net.opentsdb.core.http.rest.json.*;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.ResponseBuilder;

enum NameType
{
	METRIC_NAMES,
	TAG_KEYS,
	TAG_VALUES
}

@Path("/api/v1")
public class MetricsResource
{
	private static final Logger log = LoggerFactory.getLogger(MetricsResource.class);

	private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

	private final Datastore datastore;
	private final Map<String, DataFormatter> formatters = new HashMap<String, DataFormatter>();
	private final ObjectMapper mapper;

	@Inject
	public MetricsResource(Datastore datastore)
	{
		this.datastore = checkNotNull(datastore);
		formatters.put("json", new JsonFormatter());

		mapper = new ObjectMapper();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metricnames")
	public Response getMetricNames()
	{
		return executeNameQuery(NameType.METRIC_NAMES);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagnames")
	public Response getTagNames()
	{
		return executeNameQuery(NameType.TAG_KEYS);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagvalues")
	public Response getTagValues()
	{
		return executeNameQuery(NameType.TAG_VALUES);
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints")
	public Response add(String json)
	{
		checkNotNull(json);

		try
		{
			MetricRequestList metrics = (MetricRequestList) parseJson(MetricRequestList.class, json);

			for (NewMetricRequest metricRequest : metrics.getMetricsRequest())
			{
				DataPointSet set = new DataPointSet(metricRequest.getName());
				if (metricRequest.getValue().contains("."))
				{
					set.addDataPoint(new DataPoint(metricRequest.getTimestamp(), Double.parseDouble(metricRequest.getValue())));
				}
				else
				{
					set.addDataPoint(new DataPoint(metricRequest.getTimestamp(), Long.parseLong(metricRequest.getValue())));
				}

				Map<String, String> tags = metricRequest.getTags();
				for (String key : tags.keySet())
				{
					set.addTag(key, tags.get(key));
				}

				datastore.putDataPoints(set);
			}
			return Response.status(204).build();
		}
		catch (BeanValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addErrors(e.getErrorMessages()).build();
		}
		catch(JsonMapperParsingException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder
					.addError(e.getMessage() + ":" + e.getCause().getMessage())
					.build();

		}
		catch (Exception e)
		{
			log.error("Failed to add metric.", e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage())).build();
		}
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/query")
	public Response get(String json) throws Exception
	{
		// todo verify that end time is not before start time.
		checkNotNull(json);

		try
		{
			QueryRequest request = (QueryRequest) parseJson(QueryRequest.class, json);

			List<List<DataPointGroup>> aggregatedResults = new ArrayList<List<DataPointGroup>>();

			for (Metric metric : request.getMetrics())
			{
				QueryMetric queryMetric = new QueryMetric(getStartTime(request), request.getCacheTime(),
						metric.getName(), metric.getAggregate());
				long endTime = getEndTime(request);
				if (endTime > -1)
					queryMetric.setEndTime(endTime);
				queryMetric.setRate(metric.isRate());
				queryMetric.setGroupBy(metric.getGroupBy());
				if (metric.getSampling() != null)
				{
					queryMetric.setDownSample(metric.getSampling().getDuration(),
							metric.getSampling().getUnit(), metric.getSampling().getAggregate());
				}
				queryMetric.setTags(metric.getTags());

				aggregatedResults.add(datastore.query(queryMetric));
			}

			File cachedOutput = File.createTempFile("query", "output");
			FileWriter writer = new FileWriter(cachedOutput);

			DataFormatter formatter = formatters.get("json");
			formatter.format(writer, aggregatedResults);
			writer.flush();
			writer.close();

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(new ResponseStreamingOutput(cachedOutput));
			return responseBuilder.build();
		}
		catch (BeanValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addErrors(e.getErrorMessages()).build();
		}
		catch(JsonMapperParsingException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder
					.addError(e.getMessage() + ":" + e.getCause().getMessage())
					.build();

		}
		catch (Exception e)
		{
			log.error("Query failed.", e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage())).build();
		}
	}

	private Object parseJson(Type type, String json) throws Exception
	{
		Object object;
		try {
			JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(json);
			object = mapper.readValue(jsonParser, mapper.getTypeFactory().constructType(type));
		}
		catch (Exception e) {
			// We want to handle parsing exceptions differently than regular IOExceptions so just rethrow IOExceptions
			if (e instanceof IOException && !(e instanceof JsonProcessingException) && !(e instanceof EOFException)) {
				throw e;
			}

			log.debug("Invalid json for Java type " + MetricRequestList.class.getName(), e);

			throw new JsonMapperParsingException(MetricRequestList.class, e);
		}

		// validate object using the bean validation framework
		Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(object);
		if (!violations.isEmpty()) {
			throw new BeanValidationException(violations);
		}

		return object;
	}

	private long getStartTime(QueryRequest request)
	{
		if (request.getStartAbsolute() != null)
			return Long.parseLong(request.getStartAbsolute());
		else
			return request.getStartRelative().getTimeRelativeTo(System.currentTimeMillis());
	}

	private long getEndTime(QueryRequest request)
	{
		if (request.getEndAbsolute() != null)
			return Long.parseLong(request.getEndAbsolute());
		else if (request.getEndRelative() != null)
			return request.getEndRelative().getTimeRelativeTo(System.currentTimeMillis());
		return -1;
	}

	private Response executeNameQuery(NameType type)
	{
		try
		{
			Iterable<String> values = null;
			switch(type)
			{
				case METRIC_NAMES:
					values = datastore.getMetricNames();
					break;
				case TAG_KEYS:
					values = datastore.getTagNames();
					break;
				case TAG_VALUES:
					values = datastore.getTagValues();
					break;
			}

			File cachedOutput = File.createTempFile("query", "output");
			FileWriter writer = new FileWriter(cachedOutput);

			DataFormatter formatter = formatters.get("json");
			formatter.format(writer, values);
			writer.flush();
			writer.close();

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(new ResponseStreamingOutput(cachedOutput));
			return responseBuilder.build();
		}
		catch (Exception e)
		{
			log.error("Failed to get " + type, e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage())).build();
		}
	}

	public class ResponseStreamingOutput implements StreamingOutput
	{
		private File file;

		public ResponseStreamingOutput(File file)
		{
			this.file = file;
		}

		@SuppressWarnings("ResultOfMethodCallIgnored")
		public void write(OutputStream output) throws IOException, WebApplicationException
		{
			FileInputStream inputStream = new FileInputStream(file);
			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = inputStream.read(buffer)) != -1)
			{
				output.write(buffer, 0, bytesRead);
			}

			file.delete();
		}
	}
}
