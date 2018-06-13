package org.kairosdb.core.configuration;

import com.typesafe.config.Config;

public interface ConfigExtractor
{
	/**
	 * @param config the {@link Config} to extract from
	 * @param path the {@link Config} path
	 * @return the extracted value
	 */
	public Object extractValue(Config config, String path);

	/**
	 * @return the types this {@link ConfigExtractor} will extract for.
	 */
	public Class<?>[] getMatchingClasses();
}
