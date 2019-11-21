package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.onlineconfig.EntityResolver;
import org.kairosdb.core.scheduler.KairosDBJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.quartz.TriggerBuilder.newTrigger;

public class CacheHeatingConfigurationUpdateJob implements KairosDBJob {
    public static final Logger logger = LoggerFactory.getLogger(CacheHeatingConfigurationUpdateJob.class);
    private static final String ENTITY_ID = "kairosdb-write-cache-heating";

    private final CacheHeatingConfiguration config;
    private final EntityResolver entityResolver;
    private final String schedule;

    @Inject
    public CacheHeatingConfigurationUpdateJob(final CacheHeatingConfiguration config,
                                              final EntityResolver entityResolver,
                                              @Named("kairosdb.tiers.schedule") final String schedule) {
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
        logger.debug("Updating metric tiers configuration");
        this.getHeatingInterval().ifPresent(newInterval -> {
            logger.debug("Updating heating interval to " + newInterval);
            config.update(newInterval);
        });
        logger.debug("Config updated");
    }

    private Optional<Integer> getHeatingInterval() {
        return this.entityResolver.getEntityData(ENTITY_ID)
                .flatMap(dataNode -> this.entityResolver.getIntValue(dataNode, "heating_interval_minutes"));
    }


}
