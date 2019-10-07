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

package org.kairosdb.core.http.rest;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.MalformedJsonException;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.formatter.DataFormatter;
import org.kairosdb.core.formatter.FormatterException;
import org.kairosdb.core.formatter.JsonFormatter;
import org.kairosdb.core.formatter.JsonResponse;
import org.kairosdb.core.http.rest.json.*;
import org.kairosdb.core.http.rest.metrics.QueryMeasurementProvider;
import org.kairosdb.core.opentracing.HttpHeadersCarrier;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.core.tiers.QueryRejectedException;
import org.kairosdb.datastore.cassandra.MaxRowKeysForQueryExceededException;
import org.kairosdb.util.MemoryMonitorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.ResponseBuilder;

enum NameType {
	METRIC_NAMES,
	TAG_KEYS
}

@Path("/api/v1")
public class MetricsResource implements KairosMetricReporter {
	public static final Logger logger = LoggerFactory.getLogger(MetricsResource.class);
	public static final String QUERY_TIME = "kairosdb.http.query_time";
	public static final String REQUEST_TIME = "kairosdb.http.request_time";
	public static final String INGEST_COUNT = "kairosdb.http.ingest_count";
	public static final String INGEST_TIME = "kairosdb.http.ingest_time";
	public static final String QUERY_DATAPOINTS_REQUESTED = "kairosdb.http.datapoints_requested";
	public static final String QUERY_URL = "/datapoints/query";

	private final KairosDatastore datastore;
	private final Map<String, DataFormatter> formatters = new HashMap<String, DataFormatter>();
	private final QueryParser queryParser;

	// Used for parsing incoming metrices
	private final Gson gson;

	// These two are used to track rate of ingestion
	private final AtomicInteger m_ingestedDataPoints = new AtomicInteger();
	private final AtomicInteger m_ingestTime = new AtomicInteger();
	private final AtomicLong m_datapointsCount = new AtomicLong();

	private final KairosDataPointFactory m_kairosDataPointFactory;

	public static final String READ_TIMEOUT = "kairosdb.datastore.datapoints.read.timeout";
	public static final String ARTIFACT_VERSION = "kairosdb.datastore.artifact.version";
	public static final String DEPLOYMENT_ID = "kairosdb.datastore.deployment.id";

	@Inject(optional = true)
	@Named(READ_TIMEOUT)
	private int m_readTimeout = 30000;

	@Inject(optional = true)
	@Named(ARTIFACT_VERSION)
	private String m_artifactVersion = "2.0-z";

	@Inject(optional = true)
	@Named(DEPLOYMENT_ID)
	private String m_deploymentId = "2.0-z-d1";

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	@Named("HOSTNAME")
	private String hostName = "localhost";

	private QueryMeasurementProvider queryMeasurementProvider;

	private Tracer tracer;

	private TimeLimiter limiter;

	@Inject
	public MetricsResource(KairosDatastore datastore, QueryParser queryParser,
						   KairosDataPointFactory dataPointFactory, QueryMeasurementProvider queryMeasurementProvider, Tracer tracer) {
		this.datastore = checkNotNull(datastore);
		this.queryParser = checkNotNull(queryParser);
		this.queryMeasurementProvider = checkNotNull(queryMeasurementProvider);
		m_kairosDataPointFactory = dataPointFactory;
		formatters.put("json", new JsonFormatter());

		GsonBuilder builder = new GsonBuilder();
		gson = builder.create();

		this.tracer = tracer;
		limiter = new SimpleTimeLimiter(Executors.newCachedThreadPool());
	}

	private ResponseBuilder setHeaders(ResponseBuilder responseBuilder) {
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Pragma", "no-cache");
		responseBuilder.header("Cache-Control", "no-cache");
		responseBuilder.header("Expires", 0);

		return (responseBuilder);
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/version")
	public Response corsPreflightVersion(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										 @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/version")
	public Response getVersion() {
		Package thisPackage = getClass().getPackage();
		String versionString = thisPackage.getImplementationTitle() + " " + thisPackage.getImplementationVersion();
		ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity("{\"version\": \"" + versionString + "\"}");
		setHeaders(responseBuilder);
		return responseBuilder.build();
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metricnames")
	public Response corsPreflightMetricNames(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
											 @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metricnames")
	public Response getMetricNames() {
		return executeNameQuery(NameType.METRIC_NAMES);
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagnames")
	public Response corsPreflightTagNames(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										  @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagnames")
	public Response getTagNames() {
		return executeNameQuery(NameType.TAG_KEYS);
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints")
	public Response corsPreflightDataPoints(@HeaderParam("Access-Control-Request-Headers") String requestHeaders,
											@HeaderParam("Access-Control-Request-Method") String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Consumes("application/gzip")
	@Path("/datapoints")
	public Response addGzip(@Context HttpHeaders httpHeaders, InputStream gzip) {
		GZIPInputStream gzipInputStream;
		try {
			gzipInputStream = new GZIPInputStream(gzip);
		} catch (IOException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		return (add(httpHeaders, gzipInputStream));
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints")
	public Response add(@Context HttpHeaders httpHeaders, InputStream json) {
		Span span = createSpan("datapoints_insert", httpHeaders);

		try (Scope scope = tracer.scopeManager().activate(span, false)) {
			DataPointsParser parser = new DataPointsParser(datastore, new InputStreamReader(json, "UTF-8"),
					gson, m_kairosDataPointFactory);
			ValidationErrors validationErrors = parser.parse();

			span.log("datapoint_count: " + parser.getDataPointCount());
			m_ingestedDataPoints.addAndGet(parser.getDataPointCount());
			m_ingestTime.addAndGet(parser.getIngestTime());

			if (!validationErrors.hasErrors())
				return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
			else {
				JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
				for (String errorMessage : validationErrors.getErrors()) {
					builder.addError(errorMessage);
				}
				return builder.build();
			}
		} catch (JsonIOException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (JsonSyntaxException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (MalformedJsonException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (Exception e) {
			logger.error("Failed to add metric.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory error.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} finally {
			span.finish();
		}
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/query/tags")
	public Response corsPreflightQueryTags(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										   @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/query/tags")
	public Response getMeta(@Context HttpHeaders httpHeaders, String json) {
		checkNotNull(json);
		logger.debug(json);

		Span span = createSpan("datapoints_query_tags", httpHeaders);

		try (Scope scope = tracer.scopeManager().activate(span, false)) {
			File respFile = File.createTempFile("kairos", ".json", new File(datastore.getCacheDir()));
			BufferedWriter writer = new BufferedWriter(new FileWriter(respFile));

			JsonResponse jsonResponse = new JsonResponse(writer);

			jsonResponse.begin();

			List<QueryMetric> queries = queryParser.parseQueryMetric(json);

			for (QueryMetric query : queries) {
				List<DataPointGroup> result = datastore.queryTags(query);

				try {
					jsonResponse.formatQuery(result, false, -1);
				} finally {
					for (DataPointGroup dataPointGroup : result) {
						dataPointGroup.close();
					}
				}
			}

			jsonResponse.end();
			writer.flush();
			writer.close();

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
					new FileStreamingOutput(respFile));

			setHeaders(responseBuilder);
			return responseBuilder.build();
		} catch (JsonSyntaxException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (QueryException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (BeanValidationException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addErrors(e.getErrorMessages()).build();
		} catch (MaxRowKeysForQueryExceededException e) {
			logger.error("Query failed with too many rows", e);
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (MemoryMonitorException e) {
			logger.error("Query failed.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			System.gc();
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (QueryRejectedException e) {
			logger.error("Query was rejected.", e);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (Exception e) {
			logger.error("Query failed.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory error.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} finally {
			span.finish();
		}
	}

	/**
	 * Information for this endpoint was taken from https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS.
	 * <p/>
	 * <p/>Response to a cors preflight request to access data.
	 */
	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path(QUERY_URL)
	public Response corsPreflightQuery(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
									   @HeaderParam("Access-Control-Request-Method") final String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path(QUERY_URL)
	public Response query(@Context HttpHeaders httpHeaders, @QueryParam("query") String json) throws Exception {
		return get(httpHeaders, json);
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path(QUERY_URL)
	public Response get(@Context HttpHeaders httpHeaders, String json) throws Exception {
		checkNotNull(json);
		logger.debug(json);


		final Span span = createSpan("datapoints_query", httpHeaders);
		try (Scope scope = tracer.scopeManager().activate(span, false)) {
			File respFile = File.createTempFile("kairos", ".json", new File(datastore.getCacheDir()));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(respFile), "UTF-8"));
			JsonResponse jsonResponse = new JsonResponse(writer);
			jsonResponse.begin();
			List<QueryMetric> queries = queryParser.parseQueryMetric(json);


			return limiter.callWithTimeout(() -> {
				try (Scope internalScope = tracer.scopeManager().activate(span, false)) {

					for (QueryMetric query : queries) {
						Map<String, Collection<String>> tags = query.getTags().asMap();
						Set<String> keys = tags.keySet();

						span.setTag("metric_name", query.getName());

						for (String key : keys) {
							StringBuilder str = new StringBuilder();
							Iterator<String> iter = tags.get(key).iterator();
							while (iter.hasNext()) {
								str.append(iter.next() + ",");
							}
							str.deleteCharAt(str.lastIndexOf(","));
							span.setTag(key, str.toString());
						}

						queryMeasurementProvider.measureSpanForMetric(query);
						queryMeasurementProvider.measureDistanceForMetric(query);

						DatastoreQuery dq = datastore.createQuery(query);
						try {
							List<DataPointGroup> results = dq.execute();
							jsonResponse.formatQuery(results, query.isExcludeTags(), dq.getSampleSize());
						} catch (Throwable e) {
							queryMeasurementProvider.measureSpanError(query);
							queryMeasurementProvider.measureDistanceError(query);
							throw e;
						} finally {
							m_datapointsCount.addAndGet(dq.getSampleSize());
							queryMeasurementProvider.measureSpanSuccess(query);
							queryMeasurementProvider.measureDistanceSuccess(query);
							dq.close();
						}
					}

					jsonResponse.end();
					writer.flush();
					writer.close();

					ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
							new FileStreamingOutput(respFile));

					setHeaders(responseBuilder);
					return responseBuilder.build();
				}

			}, m_readTimeout, TimeUnit.MILLISECONDS, true);
		} catch (JsonSyntaxException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (QueryException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (BeanValidationException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addErrors(e.getErrorMessages()).build();
		} catch (MaxRowKeysForQueryExceededException e) {
			logger.error("Query failed with too many rows", e);
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (MemoryMonitorException e) {
			logger.error("Query failed.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			Thread.sleep(1000);
			System.gc();
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (IOException e) {
			logger.error("Failed to open temp folder " + datastore.getCacheDir(), e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (UncheckedTimeoutException e) {
			logger.error("Request to read datapoints timed out at " + m_readTimeout + " milli seconds", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.setTag("query_timeout", true);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (QueryRejectedException e) {
			logger.error("Query was rejected.", e);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (Exception e) {
			logger.error("Query failed.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory error.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} finally {
			span.finish();
			ThreadReporter.clear();
		}
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/delete")
	public Response corsPreflightDelete(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										@HeaderParam("Access-Control-Request-Method") final String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/delete")
	public Response delete(@Context HttpHeaders httpHeaders, String json) throws Exception {
		checkNotNull(json);
		logger.debug(json);

		Span span = createSpan("datapoints_delete", httpHeaders);

		try (Scope scope = tracer.scopeManager().activate(span, false)) {
			span = scope.span();

			List<QueryMetric> queries = queryParser.parseQueryMetric(json);

			for (QueryMetric query : queries) {
				datastore.delete(query);
			}

			return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
		} catch (JsonSyntaxException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (QueryException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addError(e.getMessage()).build();
		} catch (BeanValidationException e) {
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return builder.addErrors(e.getErrorMessages()).build();
		} catch (MemoryMonitorException e) {
			logger.error("Query failed.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			System.gc();
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (Exception e) {
			logger.error("Delete failed.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} catch (OutOfMemoryError e) {
			logger.error("Out of memory error.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} finally {
			span.finish();
		}
	}

	public static ResponseBuilder getCorsPreflightResponseBuilder(final String requestHeaders,
																  final String requestMethod) {
		ResponseBuilder responseBuilder = Response.status(Response.Status.OK);
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Access-Control-Allow-Headers", requestHeaders);
		responseBuilder.header("Access-Control-Max-Age", "86400"); // Cache for one day
		if (requestMethod != null) {
			responseBuilder.header("Access-Control-Allow_Method", requestMethod);
		}

		return responseBuilder;
	}


	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metric/{metricName}")
	public Response corsPreflightMetricDelete(@HeaderParam("Access-Control-Request-Headers") String requestHeaders,
											  @HeaderParam("Access-Control-Request-Method") String requestMethod) {
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metric/{metricName}")
	public Response metricDelete(@Context HttpHeaders httpHeaders, @PathParam("metricName") String metricName) throws Exception {
		Span span = createSpan("delete_metric", httpHeaders);

		try (Scope scope = tracer.scopeManager().activate(span, false)) {
			QueryMetric query = new QueryMetric(Long.MIN_VALUE, Long.MAX_VALUE, 0, metricName);
			datastore.delete(query);

			return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
		} catch (Exception e) {
			logger.error("Delete failed.", e);
			Tags.ERROR.set(span, Boolean.TRUE);
			span.log(e.getMessage());
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		} finally {
			span.finish();
		}
	}

	private Response executeNameQuery(NameType type) {
		try {
			Iterable<String> values = null;
			switch (type) {
				case METRIC_NAMES:
					values = datastore.getMetricNames();
					break;
				case TAG_KEYS:
					values = datastore.getTagNames();
					break;
			}

			DataFormatter formatter = formatters.get("json");

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
					new ValuesStreamingOutput(formatter, values));
			setHeaders(responseBuilder);
			return responseBuilder.build();
		} catch (Exception e) {
			logger.error("Failed to get " + type, e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
					new ErrorResponse(e.getMessage())).build();
		}
	}

	@Override
	public List<DataPointSet> getMetrics(long now) {
		int time = m_ingestTime.getAndSet(0);
		int count = m_ingestedDataPoints.getAndSet(0);
		long requestedCount = m_datapointsCount.getAndSet(0);

		DataPointSet dpsCount = new DataPointSet(INGEST_COUNT);
		DataPointSet dpsTime = new DataPointSet(INGEST_TIME);
		DataPointSet dpsRequestedCount = new DataPointSet(QUERY_DATAPOINTS_REQUESTED);

		dpsCount.addTag("host", hostName);
		dpsTime.addTag("host", hostName);
		dpsRequestedCount.addTag("host", hostName);
		dpsRequestedCount.addTag("artifact_version", m_artifactVersion);
		dpsRequestedCount.addTag("deployment_id", m_deploymentId);

		dpsCount.addDataPoint(m_longDataPointFactory.createDataPoint(now, count));
		dpsTime.addDataPoint(m_longDataPointFactory.createDataPoint(now, time));
		dpsRequestedCount.addDataPoint(m_longDataPointFactory.createDataPoint(now, requestedCount));
		List<DataPointSet> ret = new ArrayList<>();

		ret.add(dpsRequestedCount);

		if (count != 0){
			ret.add(dpsCount);
			ret.add(dpsTime);
		}

		return ret;
	}

	public class ValuesStreamingOutput implements StreamingOutput {
		private DataFormatter m_formatter;
		private Iterable<String> m_values;

		public ValuesStreamingOutput(DataFormatter formatter, Iterable<String> values) {
			m_formatter = formatter;
			m_values = values;
		}

		@SuppressWarnings("ResultOfMethodCallIgnored")
		public void write(OutputStream output) throws IOException, WebApplicationException {
			Writer writer = new OutputStreamWriter(output, "UTF-8");

			try {
				m_formatter.format(writer, m_values);
			} catch (FormatterException e) {
				logger.error("Description of what failed:", e);
			}

			writer.flush();
		}
	}

	public class FileStreamingOutput implements StreamingOutput {
		private File m_responseFile;

		public FileStreamingOutput(File responseFile) {
			m_responseFile = responseFile;
		}

		@SuppressWarnings("ResultOfMethodCallIgnored")
		@Override
		public void write(OutputStream output) throws IOException, WebApplicationException {
			try (InputStream reader = new FileInputStream(m_responseFile)) {
				byte[] buffer = new byte[1024];
				int size;

				while ((size = reader.read(buffer)) != -1) {
					output.write(buffer, 0, size);
				}

				output.flush();
			} finally {
				m_responseFile.delete();
			}
		}
	}

	public Span createSpan(String spanName, HttpHeaders httpHeaders) {
		HttpHeadersCarrier carrier = new HttpHeadersCarrier(httpHeaders.getRequestHeaders());
		SpanContext spanContext = tracer.extract(Format.Builtin.HTTP_HEADERS, carrier);
		Tracer.SpanBuilder spanBuild = tracer.buildSpan(spanName).withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER);
		if (spanContext != null)
			spanBuild.asChildOf(spanContext);

		Span span = spanBuild.start();
		span.setTag("artifact_version", m_artifactVersion);
		span.setTag("deployment_id", m_deploymentId);
		return span;
	}
}
