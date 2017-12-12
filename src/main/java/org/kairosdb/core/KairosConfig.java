package org.kairosdb.core;

import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.util.Map;
import java.util.Set;

public interface KairosConfig extends Map<String, String>
{
    enum ConfigFormat
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

    void load(File file) throws IOException;

    void load(InputStream inputStream, ConfigFormat format);

    boolean isSupportedFormat(ConfigFormat format);

    String getProperty(String key);

    void setProperty(String key, String value);

    Set<String> stringPropertyNames();
}