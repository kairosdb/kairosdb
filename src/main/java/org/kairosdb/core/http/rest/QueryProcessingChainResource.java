package org.kairosdb.core.http.rest;


import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.processingstage.QueryProcessingChain;
import org.kairosdb.core.processingstage.QueryProcessingStageFactory;
import org.kairosdb.core.processingstage.metadata.QueryProcessingStageMetadata;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

@Path("/api/v1/queryprocessing")
public class QueryProcessingChainResource
{
    private QueryProcessingChain queryProcessingChain;
    private Gson gson = new Gson();

    @Inject
    public QueryProcessingChainResource(QueryProcessingChain queryProcessingChain)
    {
        this.queryProcessingChain = queryProcessingChain;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/stages/{queryProcessorFamily}")
    public Response getQueryProcessor(@PathParam("queryProcessorFamily") String queryProcessorFamilyName)
    {
        QueryProcessingStageFactory<?> queryProcessingStageFactory = queryProcessingChain.getQueryProcessingStageFactory(queryProcessorFamilyName);
        if (queryProcessingStageFactory == null)
        {
            JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.NOT_FOUND);
            builder.addError("Unknown processing stage family '" + queryProcessorFamilyName.toLowerCase() + "'");
            return builder.build();
        }

        ImmutableList<QueryProcessorMetadata> queryProcessorMetadata = queryProcessingStageFactory.getQueryProcessorMetadata();
        Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(gson.toJson(queryProcessorMetadata));
        setHeaders(responseBuilder);
        return responseBuilder.build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/stages")
    public Response getQueryProcessingChain()
    {
        ImmutableList<QueryProcessingStageMetadata> processingChainMetadata = queryProcessingChain.getQueryProcessingChainMetadata();
        Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(gson.toJson(processingChainMetadata));
        setHeaders(responseBuilder);
        return responseBuilder.build();
    }
}
