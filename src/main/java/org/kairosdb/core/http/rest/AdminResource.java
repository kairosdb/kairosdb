package org.kairosdb.core.http.rest;

import com.google.common.collect.SetMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agileclick.genorm.runtime.Pair;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

@Path("api/v1/admin")
public class AdminResource
{
    private static final Logger logger = LoggerFactory.getLogger(AdminResource.class);

    private final QueryQueuingManager m_queuingManager;

    @Inject
    public AdminResource(QueryQueuingManager queuingManager)
    {
        this.m_queuingManager = checkNotNull(queuingManager, "queuingManager cannot be null.");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/runningqueries")
    public Response listRunningQueries()
    {
        try
        {
            int queriesWaitingCount = m_queuingManager.getQueryWaitingCount();
            ArrayList<Pair<String, QueryMetric>> runningQueries = m_queuingManager.getRunningQueries();
            JsonArray queryInfo = new JsonArray();

            JsonObject responseJson = new JsonObject();
            for (Pair<String, QueryMetric> query : runningQueries) {
                JsonObject queryJson = new JsonObject();
                String queryHash = query.getFirst();
                QueryMetric queryMetric = query.getSecond();
                queryJson.addProperty("query hash", queryHash);
                queryJson.addProperty("metric name", queryMetric.getName());
                JsonObject groupBys = new JsonObject();
                for (GroupBy groupBy : queryMetric.getGroupBys())
                {
                    groupBys.addProperty("group by", groupBy.getClass().toString());
//                    groupBys.addProperty("group by", groupBy.toString());
                }
                queryJson.add("group bys", groupBys);

                JsonObject aggs = new JsonObject();
                for (Aggregator agg : queryMetric.getAggregators())
                {
                    aggs.addProperty("aggregator", agg.getClass().toString());
//                    aggs.addProperty("aggregator", agg.toString());
                }
                queryJson.add("aggregators", aggs);

                JsonObject tags = new JsonObject();
                SetMultimap<String, String> tagSet = queryMetric.getTags();
                for (String key : tagSet.keySet())
                {
                    tags.addProperty(key, tagSet.get(key).toString());
                }
                queryJson.add("tags", tags);

                queryJson.add("query JSON", queryMetric.getJsonObj());

                queryInfo.add(queryJson);
            }
            responseJson.add("queries", queryInfo);
            responseJson.addProperty("queries waiting", queriesWaitingCount);

            Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(responseJson.toString());
            setHeaders(responseBuilder);
            return responseBuilder.build();

        }
        catch (Exception e)
        {
            logger.error("Failed to get running queries.", e);
            return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

}
