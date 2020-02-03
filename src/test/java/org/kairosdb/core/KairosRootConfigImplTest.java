package org.kairosdb.core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.kairosdb.core.KairosRootConfig.ConfigFormat;
import static org.junit.Assert.*;

public class KairosRootConfigImplTest
{

	private static final String PROPERITES_CONFIG_FILE = "config.properties";
	private static final String JSON_CONFIG_FILE = "config.json";
	private static final String HOCON_CONFIG_FILE = "config.conf";

	private static final String RESERVED_HOCON_CHARS = "${}[]:+=#`^?!@*// ";

	private static Map<String, String> defaultProperties;
	private static Map<String, String> properties;

	@BeforeClass
	public static void setupClass()
	{
		Map<String, String> map = new HashMap<>();
		map.put("key1", "default_value1");
		map.put("key3", "default_value2");
		map.put("object.key1", "default_value3");
		map.put("object.key3", "default_value4");
		map.put("object.inner_object.key1", "default_value5");
		map.put("object.inner_object.key3", "default_value6");
		defaultProperties = Collections.unmodifiableMap(map);

		map = new HashMap<>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		map.put("object.key1", "value3");
		map.put("object.key2", "value4");
		map.put("object.inner_object.key1", "value5");
		map.put("object.inner_object.key2", "value6");
		properties = Collections.unmodifiableMap(map);
	}

	private static Map<String, String> merge(Map<String, String> map1, Map<String, String> map2)
	{
		Map<String, String> mergedMap = new HashMap<>();
		mergedMap.putAll(map1);
		mergedMap.putAll(map2);
		return mergedMap;
	}

	private static void verifyContains(Map<String, String> map, KairosRootConfig config)
	{
		for (String key : map.keySet())
		{
			assertEquals(map.get(key), config.getProperty(key));
		}
	}

	private KairosRootConfig m_config;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void setup()
	{
		this.m_config = new KairosRootConfig();
	}

	private File getFile(String fileName) throws URISyntaxException
	{
		URL url = Resources.getResource(fileName);
		return Paths.get(url.toURI()).toFile();
	}

	private File makeFile(String fileName, String content) throws IOException
	{
		final File file = tempFolder.newFile(fileName);
		try (PrintWriter out = new PrintWriter(file))
		{
			out.write(content);
		}
		return file;
	}

	@Test
	public void test_getProperty()
	{
		m_config.load(ImmutableMap.of("key", "value"));
		assertEquals("value", m_config.getProperty("key"));
	}

	@Test
	public void test_getProperty_shouldReturnUpdatedValue()
	{
		m_config.load(ImmutableMap.of("key", "initialValue"));
		m_config.load(ImmutableMap.of("key", "updatedValue"));
		assertEquals("updatedValue", m_config.getProperty("key"));
	}

	@Test
	public void test_getProperty_shouldReturnNull()
	{
		assertNull(m_config.getProperty("key"));
	}


	@Test
	public void test_stringPropertyNames()
	{
		m_config.load(defaultProperties);
		assertEquals(defaultProperties.keySet(), Sets.newHashSet(m_config));
	}

	@Test
	public void test_load_propertiesFile() throws IOException, URISyntaxException
	{
		m_config.load(getFile(PROPERITES_CONFIG_FILE));
		verifyContains(properties, m_config);
	}

	/*@Test
	public void test_load_JSONFile() throws IOException, URISyntaxException
	{
		m_config.load(getFile(JSON_CONFIG_FILE));
		verifyContains(properties, m_config);
	}*/

	@Test
	public void test_load_HOCONFile() throws IOException, URISyntaxException
	{
		m_config.load(getFile(HOCON_CONFIG_FILE));
		verifyContains(properties, m_config);
	}

	@Test
	public void test_load_propertiesStream() throws IOException
	{
		try (InputStream is = Resources.getResource(PROPERITES_CONFIG_FILE).openStream())
		{
			m_config.load(is, ConfigFormat.PROPERTIES);
		}
		verifyContains(properties, m_config);
	}

	/*@Test
	public void test_load_JSONStream() throws IOException
	{
		try (InputStream is = Resources.getResource(JSON_CONFIG_FILE).openStream())
		{
			m_config.load(is, ConfigFormat.JSON);
		}
		verifyContains(properties, m_config);
	}*/

	@Test
	public void test_load_HOCONStream() throws IOException
	{
		try (InputStream is = Resources.getResource(HOCON_CONFIG_FILE).openStream())
		{
			m_config.load(is, ConfigFormat.HOCON);
		}
		verifyContains(properties, m_config);
	}

	@Test
	public void test_load_file_shouldOverrideExistingProperties() throws IOException, URISyntaxException
	{
		m_config.load(defaultProperties);
		m_config.load(getFile(HOCON_CONFIG_FILE));

		Map<String, String> expectedProperties = merge(defaultProperties, properties);
		verifyContains(expectedProperties, m_config);
	}

	@Test
	public void test_load_stream_shouldOverrideExistingProperties() throws IOException
	{
		m_config.load(defaultProperties);
		try (InputStream is = Resources.getResource(HOCON_CONFIG_FILE).openStream())
		{
			m_config.load(is, ConfigFormat.HOCON);
		}

		Map<String, String> expectedProperties = merge(defaultProperties, properties);
		verifyContains(expectedProperties, m_config);
	}

	@Test
	public void test_load_file_shouldUsePropertiesFormat() throws IOException
	{
		// Loading config data that is only valid using Java Properties format
		String configString = "key=" + RESERVED_HOCON_CHARS;
		m_config.load(makeFile("temp.properties", configString));
	}

	/*@Test(expected = ConfigException.Parse.class)
	public void test_load_file_shouldUseJSONFormat() throws IOException
	{
		// Loading config data that is only invalid using JSON format
		String configString = "key=value";
		m_config.load(makeFile("temp.json", configString));
	}*/

	@Test
	public void test_load_file_shouldUseHOCONFormat() throws IOException
	{
		// Loading config data that is only valid using HOCON format
		String configString = "{key: value}";
		m_config.load(makeFile("temp.conf", configString));
		assertEquals("value", m_config.getProperty("key"));
	}

	@Test
	public void test_load_stream_shouldUsePropertiesFormat() throws IOException
	{
		// Loading config data that is only valid using Java Properties format
		String config = "key=" + RESERVED_HOCON_CHARS;
		try (InputStream is = IOUtils.toInputStream(config, "UTF-8"))
		{
			m_config.load(is, ConfigFormat.PROPERTIES);
		}
	}

	/*@Test(expected = ConfigException.Parse.class)
	public void test_load_stream_shouldUseJSONFormat() throws IOException
	{
		// Loading config data that is only invalid using JSON format
		String config = "key=value";
		try (InputStream is = IOUtils.toInputStream(config, "UTF-8"))
		{
			m_config.load(is, ConfigFormat.JSON);
		}
	}*/

	@Test
	public void test_load_stream_shouldUseHOCONFormat() throws IOException
	{
		// Loading config data that is only valid using HOCON format
		String config = "{key: value}";
		try (InputStream is = IOUtils.toInputStream(config, "UTF-8"))
		{
			m_config.load(is, ConfigFormat.HOCON);
		}
		assertEquals("value", m_config.getProperty("key"));
	}

	@Test
	public void isSupportedFormat()
	{
		assertTrue(m_config.isSupportedFormat(KairosRootConfig.ConfigFormat.PROPERTIES));
		//assertTrue(m_config.isSupportedFormat(KairosConfig.ConfigFormat.JSON));
		assertTrue(m_config.isSupportedFormat(KairosRootConfig.ConfigFormat.HOCON));
	}


}