package org.kairosdb.core.oauth;

import com.google.inject.servlet.ServletModule;

import javax.inject.Singleton;
import java.util.Properties;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 4/18/13
 Time: 12:47 PM
 To change this template use File | Settings | File Templates.
 */
public class OAuthModule extends ServletModule
{
	public static final String CONSUMER_PREFIX = "kairosdb.oauth.consumer.";

	private ConsumerTokenStore m_tokenStore;

	public OAuthModule(Properties props)
	{
		m_tokenStore = new ConsumerTokenStore();

		for (Object key : props.keySet())
		{
			String strKey = (String)key;

			if (strKey.startsWith(CONSUMER_PREFIX))
			{
				String consumerKey = strKey.substring(CONSUMER_PREFIX.length());
				String consumerToken = (String)props.get(key);

				m_tokenStore.addToken(consumerKey, consumerToken);
			}
		}
	}

	public ConsumerTokenStore getTokenStore()
	{
		return (m_tokenStore);
	}

	@Override
	protected void configureServlets()
	{
		bind(ConsumerTokenStore.class).toInstance(m_tokenStore);

		bind(OAuthFilter.class).in(Singleton.class);
		filter("/api/*").through(OAuthFilter.class);
	}
}
