package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.codehaus.jackson.JsonNode;
import org.kairosdb.core.onlineconfig.EntityResolver;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.quartz.TriggerBuilder.newTrigger;

public class CacheWarmingUpConfigurationUpdateJob implements KairosDBJob {
    public static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpConfigurationUpdateJob.class);
    private static final String ENTITY_ID = "kairosdb-write-cache-heating";

    private final CacheWarmingUpConfiguration config;
    private final EntityResolver entityResolver;
    private final String schedule;

    @Inject
    public CacheWarmingUpConfigurationUpdateJob(final CacheWarmingUpConfiguration config,
                                              final EntityResolver entityResolver,
                                              @Named("kairosdb.cache.warmup.schedule") final String schedule) {
        this.config = config;
        this.schedule = schedule;
        this.entityResolver = entityResolver;
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
        logger.debug("Updating KairosDB warmup cache configuration");
        logger.debug("Current config is: " + config.toString());

        Optional<JsonNode> maybeData = this.entityResolver.getEntityData(ENTITY_ID);
        maybeData.flatMap(dataNode -> this.entityResolver.getBooleanValue(dataNode, "enabled"))
                .ifPresent(enabledValue -> {
                    logger.debug("Updating 'enabled' value to " + enabledValue);
                    config.setEnabled(enabledValue);
                });
        maybeData.flatMap(dataNode -> this.entityResolver.getIntValue(dataNode, "warming_up_interval_minutes"))
                .ifPresent(newInterval -> {
                    logger.debug("Updating warming-up interval to " + newInterval);
                    config.setHeatingIntervalMinutes(newInterval);
                });
        maybeData.flatMap(dataNode -> this.entityResolver.getIntValue(dataNode, "warming_up_interval_row_size"))
                .ifPresent(newRowSize -> {
                    logger.debug("Updating warming-up row size to " + newRowSize);
                    config.setRowIntervalInMinutes(newRowSize);
                });
        maybeData.flatMap(dataNode -> this.entityResolver.getBooleanValue(dataNode, "use_rps"))
                .ifPresent(shouldUseRPS -> {
                    logger.debug("Updating 'useRPS' value to " + shouldUseRPS);
                    config.setUseRPS(shouldUseRPS);
                });
        maybeData.flatMap(dataNode -> this.entityResolver.getIntValue(dataNode, "warming_up_rps"))
                .ifPresent(newRPS -> {
                    logger.debug("Updating warming-up inserts per second " + newRPS);
                    config.setWarmingUpInsertsPerSecond(newRPS);
                });
        logger.debug("Config updated: " + config.toString());
    }
}
