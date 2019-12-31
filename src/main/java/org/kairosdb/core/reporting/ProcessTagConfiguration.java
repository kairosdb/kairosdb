package org.kairosdb.core.reporting;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.kairosdb.core.KairosRootConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessTagConfiguration {
	public static final Logger logger = LoggerFactory.getLogger(ProcessTagConfiguration.class);
	private final ImmutableMap<String, String> m_processTags;

	@Inject
	public ProcessTagConfiguration(KairosRootConfig config) {
		if (config.hasPath("kairosdb.metrics.custom_tags")) {
			ConfigObject tagObject = config.getRawConfig().getObject("kairosdb.metrics.custom_tags");
			Map<String, String> tags = new HashMap<>();
			for(Map.Entry<String, ConfigValue> entry : tagObject.entrySet()) {
				tags.put(entry.getKey(), entry.getValue().unwrapped().toString());
			}
			m_processTags = ImmutableMap.copyOf(tags);
			logger.info("Using process tag set {}", m_processTags);
		} else {
			m_processTags = ImmutableMap.of();
		}
	}

	public ImmutableMap<String, String> getTags(){
		return this.m_processTags;
	}
}
