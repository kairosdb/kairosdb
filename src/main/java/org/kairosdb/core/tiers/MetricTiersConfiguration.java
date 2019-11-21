package org.kairosdb.core.tiers;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.kairosdb.core.tiers.MetricNameUtils.metricNameToCheckId;

public class MetricTiersConfiguration {

    private Set<Integer> criticalChecks = new HashSet<>();
    private Set<Integer> importantChecks = new HashSet<>();
    private Integer queryDistanceHoursLimit = -1;
    private Integer queryMaxCheckTier = -1;

    public Integer getQueryDistanceHoursLimit() {
        return queryDistanceHoursLimit;
    }

    public boolean isMetricActiveForQuery(final String metricName) {
        if (queryMaxCheckTier <= 0 || queryMaxCheckTier >= 3) {
            return true;
        }

        final Optional<Integer> maybeCheckId = metricNameToCheckId(metricName);
        if (maybeCheckId.isPresent()) {
            int checkId = maybeCheckId.get();

            if (queryMaxCheckTier == 1) return criticalChecks.contains(checkId);
            else if (queryMaxCheckTier == 2) return criticalChecks.contains(checkId) || importantChecks.contains(checkId);

            return false;
        } else {
            return false;
        }
    }

    void update(final Map<String, Set<Integer>> checkTiers,
                final Map<String, Integer> limitConfig) {
        this.criticalChecks = checkTiers.get("critical");
        this.importantChecks = checkTiers.get("important");

        this.queryDistanceHoursLimit = limitConfig.get("query_distance_hours_limit");
        this.queryMaxCheckTier = limitConfig.get("query_max_check_tier");

    }
}
