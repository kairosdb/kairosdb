/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.oauth;

import com.google.inject.servlet.ServletModule;

import javax.inject.Singleton;
import java.util.Properties;

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
