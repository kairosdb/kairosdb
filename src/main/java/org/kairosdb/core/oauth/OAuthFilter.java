package org.kairosdb.core.oauth;

import com.google.inject.Inject;
import com.sun.jersey.oauth.server.OAuthServerRequest;
import com.sun.jersey.oauth.signature.OAuthParameters;
import com.sun.jersey.oauth.signature.OAuthRequest;
import com.sun.jersey.oauth.signature.OAuthSecrets;
import com.sun.jersey.oauth.signature.OAuthSignature;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 4/18/13
 Time: 12:50 PM
 To change this template use File | Settings | File Templates.
 */
public class OAuthFilter implements Filter
{
	private ConsumerTokenStore m_tokenStore;

	@Inject
	public OAuthFilter(ConsumerTokenStore tokenStore)
	{
		m_tokenStore = tokenStore;
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException
	{
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
	{
		if (servletRequest instanceof HttpServletRequest)
		{
			System.out.println("It is http, Yipee");
			// Read the OAuth parameters from the request
			OAuthServletRequest request = new OAuthServletRequest((HttpServletRequest)servletRequest);
			OAuthParameters params = new OAuthParameters();
			params.readRequest(request);

			// Set the secret(s), against which we will verify the request
			OAuthSecrets secrets = new OAuthSecrets();
			// ... secret setting code ...

			// Check that the timestamp has not expired
			String timestampStr = params.getTimestamp();
			// ... timestamp checking code ...

			// Verify the signature
			/*try {
				if(!OAuthSignature.verify(request, params, secrets)) {
					throw new WebApplicationException(401);
				}
			} catch (OAuthSignatureException e) {
				throw new WebApplicationException(e, 401);
			}*/

			// Return the request
			//return containerRequest;
		}

		filterChain.doFilter(servletRequest, servletResponse);
	}

	@Override
	public void destroy()
	{
	}

	public class OAuthServletRequest implements OAuthRequest
	{
		private HttpServletRequest m_request;

		public OAuthServletRequest(HttpServletRequest request)
		{
			m_request = (HttpServletRequest)request;
		}


		@Override
		public String getRequestMethod()
		{
			return (m_request.getMethod());
		}

		@Override
		public String getRequestURL()
		{
			return (m_request.getRequestURL().toString());
		}

		@Override
		public Set<String> getParameterNames()
		{
			Set<String> parameterNames = new HashSet<String>();
			Enumeration<String> names = m_request.getParameterNames();

			while (names.hasMoreElements())
			{
				parameterNames.add(names.nextElement());
			}

			return (parameterNames);
		}

		@Override
		public List<String> getParameterValues(String s)
		{
			String[] values = m_request.getParameterValues(s);
			List<String> ret = new ArrayList<String>();

			for (String value : values)
			{
				ret.add(value);
			}

			return (ret);
		}

		@Override
		public List<String> getHeaderValues(String s)
		{
			Enumeration<String> values = m_request.getHeaders(s);
			List<String> ret = new ArrayList<String>();
			while (values.hasMoreElements())
			{
				ret.add(values.nextElement());
			}

			return (ret);
		}

		@Override
		public void addHeaderValue(String s, String s2) throws IllegalStateException
		{
			throw new IllegalStateException("Modifying OAuthServerRequest unsupported");
		}
	}
}
