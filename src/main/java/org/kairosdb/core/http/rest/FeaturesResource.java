package org.kairosdb.core.http.rest;


import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.FeatureProcessor;
import org.kairosdb.core.processingstage.metadata.FeatureProcessingMetadata;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

@Path("/api/v1/features")
public class FeaturesResource
{
    private FeatureProcessor m_featureProcessor;
    private Gson gson = new Gson();

    @Inject
    public FeaturesResource(FeatureProcessor featureProcessor)
    {
        this.m_featureProcessor = featureProcessor;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("{feature}")
    public Response getFeature(@PathParam("feature") String feature)
    {
        FeatureProcessingFactory<?> featureProcessingFactory = m_featureProcessor.getFeatureProcessingFactory(feature);
        if (featureProcessingFactory == null)
        {
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.NOT_FOUND);
            builder.addError("Unknown feature '" + feature.toLowerCase() + "'");
            return builder.build();
        }

        ImmutableList<FeatureProcessorMetadata> featureProcessorMetadata = featureProcessingFactory.getFeatureProcessorMetadata();
        Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(gson.toJson(featureProcessorMetadata));
        setHeaders(responseBuilder);
        return responseBuilder.build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    public Response getFeatures()
    {
        ImmutableList<FeatureProcessingMetadata> processingChainMetadata = m_featureProcessor.getFeatureProcessingMetadata();
        Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(gson.toJson(processingChainMetadata));
        setHeaders(responseBuilder);
        return responseBuilder.build();
    }
}
