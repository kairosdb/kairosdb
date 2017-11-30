package org.kairosdb.core;

import com.typesafe.config.*;

import java.io.*;
import java.util.*;

public class KairosConfigImpl extends HashMap<String, String> implements KairosConfig
{
    private static final Set<ConfigFormat> supportedFormats = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            ConfigFormat.PROPERTIES, ConfigFormat.JSON, ConfigFormat.HOCON)));

    @Override
    public void load(File file) throws IOException
    {
        try (InputStream is = new FileInputStream(file))
        {
            load(is, ConfigFormat.fromFileName(file.getName()));
        }
    }

    @Override
    public void load(InputStream inputStream, ConfigFormat format)
    {
        if (!isSupportedFormat(format))
        {
            throw new IllegalArgumentException("Config format is not supported: " + format.toString());
        }

        Reader reader = new InputStreamReader(inputStream);
        Config config = ConfigFactory.parseReader(reader, getParseOptions(format));

        for (Map.Entry<String, ConfigValue> entry : config.entrySet())
        {
            String value = (String)entry.getValue().unwrapped();
            put(entry.getKey(), value);
        }
    }

    private ConfigParseOptions getParseOptions(ConfigFormat format)
    {
        ConfigSyntax syntax = ConfigSyntax.valueOf(format.getExtension().toUpperCase());
        return ConfigParseOptions.defaults().setSyntax(syntax);
    }

    @Override
    public boolean isSupportedFormat(ConfigFormat format)
    {
        return supportedFormats.contains(format);
    }

    @Override
    public String getProperty(String key)
    {
        return get(key);
    }

    @Override
    public void setProperty(String key, String value)
    {
        put(key, value);
    }

    @Override
    public Set<String> stringPropertyNames()
    {
        return keySet();
    }
}
