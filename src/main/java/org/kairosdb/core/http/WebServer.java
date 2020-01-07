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

package org.kairosdb.core.http;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.*;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;


public class WebServer implements KairosDBService
{
	public static final Logger logger = LoggerFactory.getLogger(WebServer.class);
	public static final int LOG_RETAIN_DAYS = 30;

	public static final String JETTY_ADDRESS_PROPERTY = "kairosdb.jetty.address";
	public static final String JETTY_PORT_PROPERTY = "kairosdb.jetty.port";
	public static final String JETTY_WEB_ROOT_PROPERTY = "kairosdb.jetty.static_web_root";
	public static final String JETTY_SSL_PORT = "kairosdb.jetty.ssl.port";
	public static final String JETTY_SSL_PROTOCOLS = "kairosdb.jetty.ssl.protocols";
	public static final String JETTY_SSL_CIPHER_SUITES = "kairosdb.jetty.ssl.cipherSuites";
	public static final String JETTY_SSL_KEYSTORE_PATH = "kairosdb.jetty.ssl.keystore.path";
	public static final String JETTY_SSL_KEYSTORE_PASSWORD = "kairosdb.jetty.ssl.keystore.password";
	public static final String JETTY_SSL_TRUSTSTORE_PATH = "kairosdb.jetty.ssl.truststore.path";
	public static final String JETTY_THREADS_QUEUE_SIZE_PROPERTY = "kairosdb.jetty.threads.queue_size";
	public static final String JETTY_THREADS_MIN_PROPERTY = "kairosdb.jetty.threads.min";
	public static final String JETTY_THREADS_MAX_PROPERTY = "kairosdb.jetty.threads.max";
	public static final String JETTY_THREADS_KEEP_ALIVE_MS_PROPERTY = "kairosdb.jetty.threads.keep_alive_ms";
	public static final String JETTY_SHOW_STACKTRACE = "kairosdb.jetty.show_stacktrace";
	public static final String JETTY_AUTH_MODULE_NAME = "kairosdb.jetty.auth_module_name";
	public static final String JETTY_REQUEST_LOGGING_ENABLED = "kairosdb.jetty.request_logging.enabled";
	public static final String JETTY_REQUEST_LOGGING_RETAIN_DAYS = "kairosdb.jetty.request_logging.retain_days";
	public static final String JETTY_REQUEST_LOGGING_IGNORE_PATHS = "kairosdb.jetty.request_logging.ignore_paths";


	private InetAddress m_address;
	private int m_port;
	private String m_webRoot;
	private Server m_server;
	private int m_sslPort;
	private String[] m_cipherSuites;
	private String[] m_protocols;
	private String m_keyStorePath;
	private String m_keyStorePassword;
	private String m_trustStorePath = null;
	private ExecutorThreadPool m_pool;
	private boolean m_showStacktrace;
	private String m_authModuleName = null;
	private int m_requestLoggingRetainDays = LOG_RETAIN_DAYS;
	private boolean m_requestLoggingEnabled;
	private String[] m_loggingIgnorePaths;


	public WebServer(int port, String webRoot)
			throws UnknownHostException
	{
		this(null, port, webRoot);
	}

	@Inject
	public WebServer(@Named(JETTY_ADDRESS_PROPERTY) String address,
			@Named(JETTY_PORT_PROPERTY) int port,
			@Named(JETTY_WEB_ROOT_PROPERTY) String webRoot)
			throws UnknownHostException
	{
		checkNotNull(webRoot);

		m_port = port;
		m_webRoot = webRoot;
		m_address = InetAddress.getByName(address);
	}

	@Inject(optional = true)
	public void setSSLSettings(@Named(JETTY_SSL_PORT) int sslPort,
	                           @Named(JETTY_SSL_KEYSTORE_PATH) String keyStorePath,
	                           @Named(JETTY_SSL_KEYSTORE_PASSWORD) String keyStorePassword)
	{
		m_sslPort = sslPort;
		m_keyStorePath = checkNotNullOrEmpty(keyStorePath);
		m_keyStorePassword = checkNotNullOrEmpty(keyStorePassword);
	}

	@Inject(optional = true)
	public void setSSLSettings(@Named(JETTY_SSL_TRUSTSTORE_PATH) String truststorePath)
	{
		m_trustStorePath = checkNotNullOrEmpty(truststorePath);
	}

	@Inject(optional = true)
	public void setSSLCipherSuites(@Named(JETTY_SSL_CIPHER_SUITES) String cipherSuites)
	{
		checkNotNull(cipherSuites);
		m_cipherSuites = cipherSuites.split("\\s*,\\s*");
	}

	@Inject(optional = true)
	public void setSSLProtocols(@Named(JETTY_SSL_PROTOCOLS) String protocols)
	{
		m_protocols = protocols.split("\\s*,\\s*");
	}

	@Inject(optional = true)
	public void setThreadPool(@Named(JETTY_THREADS_QUEUE_SIZE_PROPERTY) int maxQueueSize,
	                            @Named(JETTY_THREADS_MIN_PROPERTY) int minThreads,
	                            @Named(JETTY_THREADS_MAX_PROPERTY) int maxThreads,
	                            @Named(JETTY_THREADS_KEEP_ALIVE_MS_PROPERTY) long keepAliveMs)
	{
		LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(maxQueueSize);
		ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveMs, TimeUnit.MILLISECONDS, queue);
		m_pool = new ExecutorThreadPool(threadPoolExecutor);
	}

	@Inject
	public void setJettyShowStacktrace(@Named(JETTY_SHOW_STACKTRACE) boolean showStacktrace) {
		m_showStacktrace = showStacktrace;
	}

	@Inject(optional = true)
	public void setJettyAuthModuleName(@Named(JETTY_AUTH_MODULE_NAME) String moduleName)
	{
		m_authModuleName = moduleName;
	}

	@Inject(optional = true)
	public void setJettyRequestLoggingEnabled(@Named(JETTY_REQUEST_LOGGING_ENABLED) String loggingEnabled)
	{
		m_requestLoggingEnabled = Boolean.parseBoolean(loggingEnabled);
	}

	@Inject(optional = true)
	public void setJettyRequestLoggingRetainDays(@Named(JETTY_REQUEST_LOGGING_RETAIN_DAYS) String retainDays)
	{
		m_requestLoggingRetainDays = Integer.parseInt(retainDays);
	}

	@Inject(optional = true)
	void setJettyRequestLoggingIgnorePaths(@Named(JETTY_REQUEST_LOGGING_IGNORE_PATHS) String ignorePaths)
	{
		Splitter splitter = Splitter.on(",");
		CharMatcher cm =  CharMatcher.anyOf("[]").or(CharMatcher.whitespace());
		splitter = splitter.trimResults(cm);
		List<String> ignorePathsList = splitter.splitToList(ignorePaths);
		m_loggingIgnorePaths = ignorePathsList.toArray(new String[ignorePathsList.size()]);
	}

	@Override
	public void start() throws KairosDBException
	{
		try
		{
			if (m_pool != null)
				m_server = new Server(m_pool);
			else
				m_server = new Server();

			//Error handler
			ErrorHandler errorHandler = new ErrorHandler();
			errorHandler.setShowStacks(m_showStacktrace);
			m_server.addBean(errorHandler);

			if (m_port > 0)
			{
				ServerConnector http = new ServerConnector(m_server);
				http.setHost(m_address.getHostName());
				http.setPort(m_port);
				m_server.addConnector(http);
			}

			if (m_keyStorePath != null && !m_keyStorePath.isEmpty())
				initializeSSL();

			ServletContextHandler servletContextHandler = new ServletContextHandler();
			//As of Jetty 9.4 the default alias checker allows symbolic links

			if (m_authModuleName != null)
			{
				servletContextHandler.setSecurityHandler(initializeAuth());
				servletContextHandler.setContextPath("/");
			}

			servletContextHandler.addFilter(GuiceFilter.class, "/api/*", null);
			servletContextHandler.addServlet(DefaultServlet.class, "/api/*");
			ServletHolder servletHolder = new ServletHolder("static", DefaultServlet.class);
			servletHolder.setInitParameter("resourceBase",m_webRoot);
			servletHolder.setInitParameter("dirAllowed","true");
			servletContextHandler.addServlet(servletHolder,"/");
			servletContextHandler.setWelcomeFiles(new String[]{"index.html"});

			//adding gzip handler
			GzipHandler gzipHandler = new GzipHandler();
			gzipHandler.setIncludedMimeTypes("application/json");
			gzipHandler.addIncludedMethods("GET","POST");
			gzipHandler.setIncludedPaths("/*");

			//chain handlers
			gzipHandler.setHandler(servletContextHandler);

			HandlerList handlers = new HandlerList();
			handlers.setHandlers(new Handler[]{gzipHandler, new DefaultHandler()}); //DefaultHandler only called if other handlers aren't called.
			m_server.setHandler(handlers);


			//some code for logging
			if(m_requestLoggingEnabled)
				initializeJettyRequestLogging();

			m_server.start();
		}
		catch (Exception e)
		{
			throw new KairosDBException(e);
		}
	}

	@Override
	public void stop()
	{
		try
		{
			if (m_server != null)
			{
				m_server.stop();
				m_server.join();
			}
		}
		catch (Exception e)
		{
			logger.error("Error stopping web server", e);
		}
	}

	public InetAddress getAddress()
    {
        return m_address;
    }

	private void initializeSSL()
	{
		logger.info("Using SSL");
		HttpConfiguration httpConfig = new HttpConfiguration();
		httpConfig.setSecureScheme("https");
		httpConfig.setSecurePort(m_sslPort);
		httpConfig.addCustomizer(new SecureRequestCustomizer());

		SslContextFactory sslContextFactory = new SslContextFactory();
		sslContextFactory.setKeyStorePath(m_keyStorePath);
		sslContextFactory.setKeyStorePassword(m_keyStorePassword);
		if (m_trustStorePath != null && !m_trustStorePath.isEmpty())
			sslContextFactory.setTrustStorePath(m_trustStorePath);

		if (m_cipherSuites != null && m_cipherSuites.length > 0)
			sslContextFactory.setIncludeCipherSuites(m_cipherSuites);

		if (m_protocols != null && m_protocols.length > 0)
			sslContextFactory.setIncludeProtocols(m_protocols);

		ServerConnector https = new ServerConnector(m_server, new SslConnectionFactory(sslContextFactory, "http/1.1"), new HttpConnectionFactory(httpConfig));
		https.setPort(m_sslPort);
		m_server.addConnector(https);
	}

	private SecurityHandler initializeAuth() throws Exception
	{
		Constraint constraint = new Constraint();
		constraint.setName(Constraint.__BASIC_AUTH);
		constraint.setRoles(new String[]{Constraint.ANY_AUTH}); //authentication is all that's supported so this allows any role.
		constraint.setAuthenticate(true);

		Constraint noConstraint = new Constraint();

		ConstraintMapping healthcheckConstraintMapping = new ConstraintMapping();
		healthcheckConstraintMapping.setConstraint(noConstraint);
		healthcheckConstraintMapping.setPathSpec("/api/v1/health/*");

		ConstraintMapping cm = new ConstraintMapping();
		cm.setConstraint(constraint);
		cm.setPathSpec("/*");

		ConstraintSecurityHandler csh = new ConstraintSecurityHandler();
		JAASLoginService l = new JAASLoginService();
		l.setLoginModuleName(m_authModuleName);
		csh.addConstraintMapping(healthcheckConstraintMapping);
		csh.addConstraintMapping(cm);
		csh.setLoginService(l);
		l.start();
		return csh;
    }

    private void initializeJettyRequestLogging()
	{
		NCSARequestLog requestLog = new NCSARequestLog("log/jetty-yyyy_mm_dd.request.log");
		requestLog.setAppend(true);
		requestLog.setExtended(false);
		requestLog.setLogTimeZone("UTC");
		requestLog.setLogLatency(true);
		requestLog.setRetainDays(m_requestLoggingRetainDays);
		if(m_loggingIgnorePaths != null)
			requestLog.setIgnorePaths(m_loggingIgnorePaths);
		m_server.setRequestLog(requestLog);
	}
}
