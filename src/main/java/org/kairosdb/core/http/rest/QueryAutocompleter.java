package org.kairosdb.core.http.rest;

import com.google.common.collect.SetMultimap;
import org.kairosdb.core.datastore.QueryMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

public class QueryAutocompleter {
    private static final Logger logger = LoggerFactory.getLogger(QueryAutocompleter.class);

    private static final String METRIC_TAG_NAME = "metric";
    private static final String KEY_TAG_NAME = "key";
    private static final LocalDateTime START_TIME_METRIC_SPLIT_WRITE = LocalDateTime.parse("2019-11-11T00:00:00");

    public void complete(QueryMetric query) {
        // TODO: Drop query start time checking after 2019-01-16
        LocalDateTime queryStart = LocalDateTime.ofInstant(Instant.ofEpochMilli(query.getStartTime()),
                TimeZone.getDefault().toZoneId());
        if (!query.getTags().containsKey(METRIC_TAG_NAME) && START_TIME_METRIC_SPLIT_WRITE.isBefore(queryStart)) {
            completeMetricTag(query);
        }
    }

    private void completeMetricTag(QueryMetric query) {
        final SetMultimap<String, String> tags = query.getTags();
        final Set<String> keys = tags.get(KEY_TAG_NAME);
        final Set<String> metrics = new HashSet<>();
        for (final String key : keys) {
            final String metric = extractMetricName(key);
            if (isWildcard(metric)) {
                return;
            }
            metrics.add(metric);
        }
        tags.putAll(METRIC_TAG_NAME, metrics);
    }

    private String extractMetricName(final String key) {
        if (null == key || "".equals(key)) return null;
        try {
            final String[] keyParts = key.split("\\.");
            final String metricName = keyParts[keyParts.length - 1];
            return "".equals(metricName) ? keyParts[keyParts.length - 2] : metricName;
        } catch (Exception e) {
            logger.warn("Problem while parsing key: " + key, e);
            return null;
        }
    }


    private boolean isWildcard(final String metric) {
        return metric == null || metric.contains("*") || metric.contains("?");
    }

}
