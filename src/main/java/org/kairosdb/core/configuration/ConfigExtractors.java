package org.kairosdb.core.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum ConfigExtractors implements ConfigExtractor
{
	BOOLEAN(boolean.class, Boolean.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getBoolean(path);
				}
			},
	BYTE(byte.class, Byte.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return (byte) config.getInt(path);
				}
			},
	SHORT(short.class, Short.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return (short) config.getInt(path);
				}
			},
	INTEGER(int.class, Integer.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getInt(path);
				}
			},
	LONG(long.class, Long.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getLong(path);
				}
			},
	FLOAT(float.class, Float.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return (float) config.getDouble(path);
				}
			},
	DOUBLE(double.class, Double.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getDouble(path);
				}
			},
	STRING(String.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getString(path);
				}
			},
	ANY_REF(Object.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getAnyRef(path);
				}
			},
	CONFIG(Config.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getConfig(path);
				}
			},
	CONFIG_OBJECT(ConfigObject.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getObject(path);
				}
			},
	CONFIG_VALUE(ConfigValue.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getValue(path);
				}
			},
	CONFIG_LIST(ConfigList.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getList(path);
				}
			},
	DURATION(Duration.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getDuration(path);
				}
			},
	MEMORY_SIZE(ConfigMemorySize.class)
			{
				@Override
				public Object extractValue(Config config, String path)
				{
					return config.getMemorySize(path);
				}
			};

	private final Class<?>[] matchingClasses;
	private static final Map<Class<?>, ConfigExtractor> EXTRACTOR_MAP = new HashMap<>();

	static
	{
		for (ConfigExtractor extractor : ConfigExtractors.values())
		{
			for (Class<?> clazz : extractor.getMatchingClasses())
			{
				EXTRACTOR_MAP.put(clazz, extractor);
			}
		}
	}

	private ConfigExtractors(Class<?>... matchingClasses)
	{
		this.matchingClasses = matchingClasses;
	}

	public Class<?>[] getMatchingClasses()
	{
		return matchingClasses;
	}

	/**
	 @param config     the {@link Config} to extract from
	 @param paramClass the class
	 @param path
	 @return the extracted config value, or empty if there was no matching {@link ConfigExtractor}.
	 */
	public static Optional<Object> extractConfigValue(Config config, Class<?> paramClass, String path)
	{
		if (EXTRACTOR_MAP.containsKey(paramClass))
		{
			return Optional.of(EXTRACTOR_MAP.get(paramClass).extractValue(config, path));
		}
		else
		{
			return Optional.empty();
		}
	}
}
