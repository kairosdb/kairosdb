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

public class CacheWarmingUpLeakingBucketJob implements KairosDBJob {
    public static final Logger logger = LoggerFactory.getLogger(CacheWarmingUpLeakingBucketJob.class);

    private final CacheWarmingUpLeakingBucketHolder leakingBucketHolder;
    private final CacheWarmingUpConfiguration config;
    private final String schedule;

    @Inject
    public CacheWarmingUpLeakingBucketJob(final CacheWarmingUpLeakingBucketHolder leakingBucketHolder,
                                          final CacheWarmingUpConfiguration config,
                                          @Named("kairosdb.cache.warmup.leaking.bucket.refill.schedule") final String schedule) {
        this.leakingBucketHolder = leakingBucketHolder;
        this.config = config;
        this.schedule = schedule;
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
        leakingBucketHolder.refillBucket(config.getWarmingUpInsertsPerSecond());
    }
}
