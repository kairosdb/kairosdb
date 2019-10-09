package org.kairosdb.core.tiers;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.stups.tokens.AccessTokens;

import java.io.IOException;
import java.util.*;

import static org.quartz.TriggerBuilder.newTrigger;

public class MetricTiersConfigurationUpdateJob implements KairosDBJob {
    public static final Logger logger = LoggerFactory.getLogger(MetricTiersConfigurationUpdateJob.class);

    private MetricTiersConfiguration config;
    private ObjectMapper objectMapper;
    private Executor executor;
    private AccessTokens accessTokens;
    private String schedule;
    private String hostname;

    MetricTiersConfigurationUpdateJob() {
    }

    @Inject
    public MetricTiersConfigurationUpdateJob(final MetricTiersConfiguration config,
                                             final ObjectMapper objectMapper,
                                             final Executor executor,
                                             final AccessTokens accessTokens,
                                             @Named("kairosdb.tiers.schedule") final String schedule,
                                             @Named("zmon.hostname") final String hostname) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.executor = executor;
        this.accessTokens = accessTokens;
        this.schedule = schedule;
        this.hostname = hostname;
    }

    @Override
    public Trigger getTrigger() {
        return (newTrigger()
                .withIdentity(this.getClass().getSimpleName())
                .withSchedule(CronScheduleBuilder.cronSchedule(schedule))
                .build());
    }

    @Override
    public void execute(final JobExecutionContext ctx) {
        logger.debug("Updating metric tiers configuration");
        try {
            final Map<String, Set<Integer>> checkTiers = getCheckTiers();
            final Map<String, Integer> limitConfig = getLimitConfig();
            config.update(checkTiers, limitConfig);
        } catch (IOException e) {
            logger.error("Metric tiers configuration update failed", e);
        }
    }

    Map<String, Set<Integer>> getCheckTiers() throws IOException {
        final Optional<JsonNode> data = getEntityData("zmon-check-tiers");

        final Set<Integer> criticalChecks = getCheckList(data, "critical");
        final Set<Integer> importantChecks = getCheckList(data, "important");

        return ImmutableMap.of("critical", criticalChecks, "important", importantChecks);
    }

    Map<String, Integer> getLimitConfig() throws IOException {
        final Optional<JsonNode> data = getEntityData("zmon-service-level-config");
        final Integer queryDistanceHoursLimit = getIntValue(data, "query_distance_hours_limit");
        final Integer queryMaxCheckTier = getIntValue(data, "query_max_check_tier");
        return ImmutableMap.of(
                "query_distance_hours_limit", queryDistanceHoursLimit,
                "query_max_check_tier", queryMaxCheckTier
        );
    }

    Optional<JsonNode> getEntityData(final String entityId) throws IOException {
        final String uri = hostname + "/api/v1/entities/" + entityId;
        final Request request = Request.Get(uri).addHeader("Authorization", "Bearer " + accessTokens.get("zmon-read"));
        final String response = executor.execute(request).returnContent().toString();
        final JsonNode jsonNode = objectMapper.readTree(response);
        return Optional.ofNullable(jsonNode.get("data"));
    }

    private Set<Integer> getCheckList(final Optional<JsonNode> data, final String kind) {
        final Iterator<JsonNode> checks = data.map(d -> d.get(kind)).map(JsonNode::iterator).orElse(Collections.emptyIterator());
        final Set<Integer> checkSet = new HashSet<>();
        while (checks.hasNext()) {
            checkSet.add(checks.next().asInt());
        }
        return checkSet;
    }

    private Integer getIntValue(Optional<JsonNode> data, String key) {
        return data.map(d -> d.get(key)).map(JsonNode::asInt).orElse(0);
    }
}
