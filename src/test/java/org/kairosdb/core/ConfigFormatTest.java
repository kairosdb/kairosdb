package org.kairosdb.core;

import org.junit.Test;

import static org.kairosdb.core.KairosRootConfig.ConfigFormat;
import static org.junit.Assert.*;

public class ConfigFormatTest {
    @Test
    public void getExtension() throws Exception
    {
        assertEquals("properties", ConfigFormat.PROPERTIES.getExtension());
        assertEquals("json", ConfigFormat.JSON.getExtension());
        assertEquals("conf", ConfigFormat.HOCON.getExtension());
    }

    @Test
    public void fromFileName() throws Exception
    {
        assertEquals(ConfigFormat.PROPERTIES, ConfigFormat.fromFileName("config.properties"));
        assertEquals(ConfigFormat.JSON, ConfigFormat.fromFileName("config.json"));
        assertEquals(ConfigFormat.HOCON, ConfigFormat.fromFileName("config.conf"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void fromFileName_withInvalidFileType() throws Exception
    {
        ConfigFormat.fromFileName("config.yaml");
    }

    @Test
    public void fromExtension() throws Exception
    {
        assertEquals(ConfigFormat.PROPERTIES, ConfigFormat.fromExtension("properties"));
        assertEquals(ConfigFormat.JSON, ConfigFormat.fromExtension("json"));
        assertEquals(ConfigFormat.HOCON, ConfigFormat.fromExtension("conf"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void fromExtension_withInvalidExtension() throws Exception
    {
        ConfigFormat.fromExtension("yaml");
    }
}