package org.kairosdb.core.oauth;

import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 4/18/13
 Time: 4:17 PM
 To change this template use File | Settings | File Templates.
 */
public class OAuthModuleTest
{
	@Test
	public void testReadingProperties()
	{
		Properties props = new Properties();

		props.put("kairosdb.oauth.consumer.cust1", "ABC123");
		props.put("kairosdb.oauth.consumerNot.cust1", "XYZ");
		props.put("kairosdb.oauth.consumer.cust2", "EFG789");

		OAuthModule module = new OAuthModule(props);

		ConsumerTokenStore tokenStore = module.getTokenStore();

		assertThat(tokenStore.getConsumerKeys().size(), is(2));
		assertThat(tokenStore.getToken("cust1"), is("ABC123"));
		assertThat(tokenStore.getToken("cust2"), is("EFG789"));
	}
}
