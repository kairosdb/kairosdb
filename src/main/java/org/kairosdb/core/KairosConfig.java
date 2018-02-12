package org.kairosdb.core;

import com.typesafe.config.Config;
import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.List;

public class KairosConfig
{
	public enum ConfigFormat
	{
		PROPERTIES("properties"),
		JSON("json"),
		HOCON("conf");

		private String extension;

		ConfigFormat(String extension)
		{
			this.extension = extension;
		}

		public String getExtension()
		{
			return this.extension;
		}

		public static ConfigFormat fromFileName(String fileName)
		{
			return fromExtension(FilenameUtils.getExtension(fileName));
		}

		public static ConfigFormat fromExtension(String extension)
		{
			for (ConfigFormat format : ConfigFormat.values())
			{
				if (extension.equals(format.getExtension()))
				{
					return format;
				}
			}
			throw new IllegalArgumentException(extension + ": invalid config extension");
		}
	}

	protected Config m_config;

	public KairosConfig()
	{
	}

	public KairosConfig(Config config)
	{
		m_config = config;
	}

	public Config getRawConfig()
	{
		return m_config;
	}

	public KairosConfig getConfig(String path)
	{
		return new KairosConfig(m_config.getConfig(path));
	}

	public boolean hasPath(String path)
	{
		return m_config.hasPath(path);
	}

	public List<KairosConfig> getConfigList(String path)
	{
		List<? extends Config> configList = m_config.getConfigList(path);
		ArrayList<KairosConfig> ret = new ArrayList<>();

		for (Config config : configList)
		{
			ret.add(new KairosConfig(config));
		}

		return ret;
	}

	public String getString(String path)
	{
		return m_config.getString(path);
	}

	public String getString(String path, String def)
	{
		if (m_config.hasPath(path))
			return m_config.getString(path);
		else
			return def;
	}

	public boolean getBoolean(String path)
	{
		return m_config.getBoolean(path);
	}

	public boolean getBoolean(String path, boolean def)
	{
		if (m_config.hasPath(path))
			return m_config.getBoolean(path);
		else
			return def;
	}

	public int getInt(String path)
	{
		return m_config.getInt(path);
	}

	public int getInt(String path, int def)
	{
		if (m_config.hasPath(path))
			return m_config.getInt(path);
		else
			return def;
	}

	public List<String> getStringList(String path)
	{
		return m_config.getStringList(path);
	}

	public List<String> getStringList(String path, List<String> def)
	{
		if (m_config.hasPath(path))
			return m_config.getStringList(path);
		else
			return def;
	}




}
