package org.kairosdb.core.tiers;

import java.util.Optional;

public class MetricNameUtils {
    public static Optional<Integer> metricNameToCheckId(final String metricName) {
        final String[] split = metricName.split("\\.");
        if (split.length != 3 || !split[0].equals("zmon") || !split[1].equals("check")) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(split[2]));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

}
