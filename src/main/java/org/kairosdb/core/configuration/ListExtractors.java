package org.kairosdb.core.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.ConfigObject;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public enum ListExtractors implements ListExtractor
{
	BOOLEAN(Boolean.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getBooleanList(path);
		}
	},
	INTEGER(Integer.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getIntList(path);
		}
	},
	DOUBLE(Double.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getDoubleList(path);
		}
	},
	LONG(Long.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getLongList(path);
		}
	},
	STRING(String.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getStringList(path);
		}
	},
	DURATION(Duration.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getDurationList(path);
		}
	},
	MEMORY_SIZE(ConfigMemorySize.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getMemorySizeList(path);
		}
	},
	OBJECT(Object.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getAnyRefList(path);
		}
	},
	CONFIG(Config.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getConfigList(path);
		}
	},
	CONFIG_OBJECT(ConfigObject.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getObjectList(path);
		}
	},
	CONFIG_VALUE(ConfigObject.class) {
		@Override
		public List<?> extractListValue(Config config, String path) {
			return config.getList(path);
		}
	}
	;

	private final Class<?> parameterizedTypeClass;
	private static final Map<Type, ListExtractor> EXTRACTOR_MAP = new HashMap<>();

	static {
		for (ListExtractor extractor : ListExtractors.values()) {
			EXTRACTOR_MAP.put(extractor.getMatchingParameterizedType(), extractor);
		}
	}

	private ListExtractors(Class<?> parameterizedTypeClass) {
		this.parameterizedTypeClass = parameterizedTypeClass;
	}

	@Override
	public Type getMatchingParameterizedType() {
		return parameterizedTypeClass;
	}

	public static Optional<List<?>> extractConfigListValue(Config config, Type listType, String path) {
		if (EXTRACTOR_MAP.containsKey(listType)) {
			return Optional.of(EXTRACTOR_MAP.get(listType).extractListValue(config, path));
		} else {
			return Optional.empty();
		}
	}
}
