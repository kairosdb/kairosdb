package org.kairosdb.core.health;

import java.util.List;

public interface HealthCheckService
{
	List<HealthStatus> getChecks();
}
