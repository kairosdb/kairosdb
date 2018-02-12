package org.kairosdb.core.oauth;

import org.junit.Test;
import org.kairosdb.core.oauth.OAuthFilter.OAuthServletRequest;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OAuthServletRequestTest
{

	@Test
	public void test_getParameterValues()
	{
		String[] parameters = new String[]{"a", "b"};
		HttpServletRequest mockServletRequest = mock(HttpServletRequest.class);
		when(mockServletRequest.getParameterValues(anyString())).thenReturn(parameters);

		OAuthServletRequest oAuthServletRequest = new OAuthServletRequest(mockServletRequest);

		List<String> parameterValues = oAuthServletRequest.getParameterValues("foo");

		assertThat(parameterValues, equalTo(Arrays.asList(parameters)));

	}
}