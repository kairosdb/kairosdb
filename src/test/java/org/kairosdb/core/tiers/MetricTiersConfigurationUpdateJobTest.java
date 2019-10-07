package org.kairosdb.core.tiers;

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MetricTiersConfigurationUpdateJobTest {
    private static final String TIERS_JSON = "{\"critical\": [10053, 10423, 10636], \"important\": [11707, 13165, 15433]}";
    private static final String LIMITS_JSON = "{\"query_max_check_tier\": 3, \"ingest_max_check_tier\": 3, \"query_distance_hours_limit\": 12}}";
    private MetricTiersConfigurationUpdateJob job;

    @Before
    public void setUp() throws Exception {
        job = spy(MetricTiersConfigurationUpdateJob.class);

        ObjectMapper mapper = new ObjectMapper();

        doReturn(Optional.of(mapper.readTree(TIERS_JSON))).when(job).getEntityData(eq("zmon-check-tiers"));
        doReturn(Optional.of(mapper.readTree(LIMITS_JSON))).when(job).getEntityData(eq("zmon-service-level-config"));
    }

    @Test
    public void getCheckTiers() throws IOException {
        final Map<String, Set<Integer>> checkTiers = job.getCheckTiers();
        assertEquals(ImmutableMap.of(
                "critical", newHashSet(10053, 10423, 10636),
                "important", newHashSet(11707, 13165, 15433)
        ), checkTiers);
    }

    @Test
    public void getLimitConfig() throws IOException {
        final Map<String, Integer> limitConfig = job.getLimitConfig();
        assertEquals(ImmutableMap.of(
                "query_max_check_tier", 3,
                "query_distance_hours_limit", 12
        ), limitConfig);
    }
}