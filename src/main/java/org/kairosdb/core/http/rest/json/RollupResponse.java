package org.kairosdb.core.http.rest.json;

import java.util.HashMap;
import java.util.Map;

public class RollupResponse
{
	private String id;
	private String name;
	private Map<String, String> attributes = new HashMap<String, String>();

	public RollupResponse(String id, String name, String url)
	{
		this.id = id;
		this.name = name;
		attributes.put("url", url);
	}

	public String getId()
	{
		return id;
	}

	public String getName()
	{
		return name;
	}

	public Map<String, String> getAttributes()
	{
		return attributes;
	}
}
