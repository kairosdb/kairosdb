package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.datastore.cassandra.CassandraConfiguration;

public class RowKeyCacheConfiguration
{

	private static final String DEFAULT_TTL_IN_SECONDS = "kairosdb.datastore.cassandra.cache.default_ttl_in_seconds";

	@Inject(optional=true)
	@Named(DEFAULT_TTL_IN_SECONDS)
	private int defaultTtlInSeconds = 86_400; // 1 day

	@Inject(optional=true)
	@Named(CassandraConfiguration.ROW_KEY_CACHE_SIZE_PROPERTY)
	private int maxSize = 16_000_000;

	public int getDefaultTtlInSeconds() {
		return defaultTtlInSeconds;
	}

	public int getMaxSize() {
		return maxSize;
	}
}
