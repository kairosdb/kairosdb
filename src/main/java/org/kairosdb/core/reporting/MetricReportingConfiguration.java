package org.kairosdb.core.reporting;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class MetricReportingConfiguration {
    private static final String ENABLED = "kairosdb.core.metrics.reporting_enabled";

    @Inject(optional=true)
    @Named(ENABLED)
    private boolean enabled  = true;

    public boolean isEnabled() {
        return enabled;
    }
}
