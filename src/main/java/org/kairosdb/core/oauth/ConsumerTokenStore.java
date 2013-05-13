package org.kairosdb.core.oauth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 4/18/13
 Time: 4:01 PM
 To change this template use File | Settings | File Templates.
 */
public class ConsumerTokenStore
{
	private Map<String, String> m_tokenMap;

	public ConsumerTokenStore()
	{
		m_tokenMap = new HashMap<String, String>();
	}

	public void addToken(String key, String token)
	{
		m_tokenMap.put(key, token);
	}

	public String getToken(String consumerKey)
	{
		return (m_tokenMap.get(consumerKey));
	}

	public Set<String> getConsumerKeys()
	{
		return (m_tokenMap.keySet());
	}
}
