package org.kairosdb.core.oauth;

import com.google.inject.Inject;
import com.sun.jersey.oauth.server.OAuthServerRequest;
import com.sun.jersey.oauth.signature.*;
import org.kairosdb.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
	public static final Logger logger = LoggerFactory.getLogger(OAuthFilter.class);

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
		HttpServletRequest httpRequest = (HttpServletRequest)servletRequest;
		HttpServletResponse httpResponse = (HttpServletResponse)servletResponse;

		//Skip oauth for local connections
		if (!"127.0.0.1".equals(servletRequest.getRemoteAddr()))
		{
			// Read the OAuth parameters from the request
			OAuthServletRequest request = new OAuthServletRequest(httpRequest);
			OAuthParameters params = new OAuthParameters();
			params.readRequest(request);

			String consumerKey = params.getConsumerKey();

			// Set the secret(s), against which we will verify the request
			OAuthSecrets secrets = new OAuthSecrets();
			secrets.setConsumerSecret(m_tokenStore.getToken(consumerKey));

			// Check that the timestamp has not expired
			String timestampStr = params.getTimestamp();
			if (timestampStr == null)
			{
				logger.warn("Missing OAuth headers");
				httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Missing OAuth headers");
				return;
			}

			long msgTime = Util.parseLong(timestampStr) * 1000L; //Message time is in seconds
			long currentTime = System.currentTimeMillis();

			//if the message is older than 5 min it is no good
			if (Math.abs(msgTime - currentTime) > 300000)
			{
				logger.warn("OAuth message time out, msg time: "+msgTime+" current time: "+currentTime);
				httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Message expired");
				return;
			}

			// Verify the signature
			try
			{
				if(!OAuthSignature.verify(request, params, secrets))
				{
					logger.warn("Invalid OAuth signature");

					httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid OAuth signature");
					return;
				}
			}
			catch (OAuthSignatureException e)
			{
				logger.warn("OAuth exception", e);

				httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid OAuth request");
				return;
			}
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
