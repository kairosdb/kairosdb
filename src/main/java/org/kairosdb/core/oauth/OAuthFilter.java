package org.kairosdb.core.oauth;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.kairosdb.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.sun.jersey.oauth.signature.OAuthParameters;
import com.sun.jersey.oauth.signature.OAuthRequest;
import com.sun.jersey.oauth.signature.OAuthSecrets;
import com.sun.jersey.oauth.signature.OAuthSignature;
import com.sun.jersey.oauth.signature.OAuthSignatureException;

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

	public static class OAuthServletRequest implements OAuthRequest
	{
		private HttpServletRequest m_request;

		public OAuthServletRequest(HttpServletRequest request)
		{
			m_request = request;
		}

		@Override
		public String getRequestMethod()
		{
			return (m_request.getMethod());
		}

		@Override
        public URL getRequestURL()
		{
            try {
                return new URL(m_request.getRequestURL().toString());
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
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

			Collections.addAll(ret, values);

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
