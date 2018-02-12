package org.kairosdb.core;

import com.typesafe.config.*;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.util.*;

public class KairosRootConfig extends KairosConfig implements Iterable<String>
{

	private static final Set<ConfigFormat> supportedFormats = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
			ConfigFormat.PROPERTIES, ConfigFormat.HOCON)));


	private final Set<String> m_keys = new TreeSet<>();

	public KairosRootConfig()
	{
		super();
	}

	public void load(File file) throws IOException
	{
		System.out.println("Loading "+file.getAbsolutePath());
		try (InputStream is = new FileInputStream(file))
		{
			load(is, ConfigFormat.fromFileName(file.getName()));
		}
	}

	public void load(InputStream inputStream, ConfigFormat format)
	{
		if (!isSupportedFormat(format))
		{
			throw new IllegalArgumentException("Config format is not supported: " + format.toString());
		}

		Reader reader = new InputStreamReader(inputStream);
		Config config = ConfigFactory.parseReader(reader, getParseOptions(format));

		addConfig(config);
	}

	public void loadSystemProperties()
	{
		Config config = ConfigFactory.systemProperties();
		addConfig(config);
	}

	public void load(Map<String, ? extends Object> map)
	{
		Config config = ConfigFactory.parseMap(map);
		addConfig(config);
	}

	private void addConfig(Config config)
	{
		if (m_config != null)
			m_config = config.withFallback(m_config);
		else
			m_config = config;

		for (Map.Entry<String, ConfigValue> entry : m_config.entrySet())
		{
			m_keys.add(entry.getKey());
		}
	}

	public void resolve()
	{
		m_config = m_config.resolve();
	}

	private ConfigParseOptions getParseOptions(ConfigFormat format)
	{
		ConfigSyntax syntax = ConfigSyntax.valueOf(format.getExtension().toUpperCase());
		return ConfigParseOptions.defaults().setSyntax(syntax);
	}

	public boolean isSupportedFormat(ConfigFormat format)
	{
		return supportedFormats.contains(format);
	}

	public String getProperty(String key)
	{
		try
		{
			return (m_config == null ? null : m_config.getString(key));
		}
		catch (ConfigException e)
		{
			return null;
		}
	}

	@Override
	public Iterator<String> iterator()
	{
		return m_keys.iterator();
	}


}