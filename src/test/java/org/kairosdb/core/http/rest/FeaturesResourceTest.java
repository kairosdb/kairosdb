package org.kairosdb.core.http.rest;

import org.junit.Test;
import org.kairosdb.testing.JsonResponse;

import java.io.IOException;

import static org.kairosdb.core.http.rest.MetricsResourceTest.assertResponse;

public class FeaturesResourceTest extends ResourceBase
{
    private static final String FEATURE_PROCESSING_URL = "http://localhost:9001/api/v1/features/";

    @Test
    public void testGetAggregatorList() throws IOException
    {
        JsonResponse response = client.get(FEATURE_PROCESSING_URL + "aggregators");
        assertResponse(response, 200, "[]");
    }

    @Test
    public void testGetInvalidFeature() throws IOException
    {
        JsonResponse response = client.get(FEATURE_PROCESSING_URL + "intel");
        assertResponse(response, 404, "{\"errors\":[\"Unknown feature 'intel'\"]}");
    }

    @Test
    public void getTestGetFeatures() throws IOException
    {
        JsonResponse response = client.get(FEATURE_PROCESSING_URL);
        assertResponse(response, 200, "[{\"name\":\"group_by\",\"label\":\"Test GroupBy\",\"properties\":[]},{\"name\":\"aggregators\",\"label\":\"Test Aggregator\",\"properties\":[]}]");
    }
}
